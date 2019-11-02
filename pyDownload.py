#-- 批量下载网络文件，若是下载失败，则再次运行继续下载

import os,asyncio,aiohttp,aiofiles,threading,time,requests,sys
from progressbar import *
from queue import Queue
from urllib.parse import urlsplit,unquote,quote
from configparser import RawConfigParser

# 设置缓存目录
download_dir = os.path.expanduser('~/AppData/Roaming/myDownload')
os.makedirs(download_dir,exist_ok=True)

class Download(object):
	def __init__(self,urls=[],max_tries=5,max_tasks=32):
		self.urls = urls
		self.headers = {}
		self.max_tasks = max_tasks
		self.max_tries = max_tries
		self.new_loop = asyncio.new_event_loop()
		# self.loop = asyncio.get_event_loop()
		self.session = aiohttp.ClientSession(loop=self.new_loop)
		# self.queue_done = asyncio.Queue(loop=self.new_loop)
		self.queue_task = Queue() 	# 每个任务的下载消息

		self.total_chunk = 0  #下载总块数
		self.total_size = 0 #文件总字节大小
		self.done_chunk = 0   #下载完成块数
		self.done_size = 0		# 下载完成字节数

		self.rs = requests.session()
		self.semaphore = asyncio.Semaphore(500)		 # 限制并发量为500,这里windows需要进行并发限制

		# 配置文件初始化
		self.cfg = RawConfigParser()
		self.conf = download_dir+'/fastdawnload.conf'
		if not os.path.exists(self.conf):
			open(self.conf, 'a').close()
		self.cfg.read(self.conf)

		# 下载进度初始化
		self.download_progress = {}	# 保存每一个下载任务
		for section in self.cfg.sections():
			if self.cfg.get(section,'success') != "2":	# 1表示下载完成,2表示合并完成
				self.download_progress[section] = {}
				self.download_progress[section]['total'] = self.cfg.get(section,'total')
				self.download_progress[section]['done'] = 0
				self.download_progress[section]['fail'] = 0
				self.download_progress[section]['content_length'] = self.cfg.get(section,'content_length')
				self.download_progress[section]['done_length'] = self.cfg.get(section,'done_length')

		# 启动线程
		t = threading.Thread(target=self.start_loop,args=(self.new_loop,))
		t.setDaemon(True)    # 设置子线程为守护线程,即主线程退出时，子线程也会跟着退出，否则主线程要等子线程结束才会退出
		t.start()

		# 开始运行
		self.run()

	def start_loop(self,loop):
		'''为子线程设置事件循环'''
		print('开启子线程 tid:',threading.currentThread().ident)
		asyncio.set_event_loop(loop)
		loop.run_forever()

	async def stop_loop(self):
		'''子线程停止事件循环'''
		print('关闭子线程 tid:',threading.currentThread().ident)
		await self.session.close()
		self.new_loop.stop()

	def check_url(self,urls):
		'''检查文件下载地址'''
		if not urls:	# 如果没有下载链接
			for section in self.cfg.sections():
				if self.cfg.get(section,'success') != '2':	# 继续下载
					urls.append(unquote(self.cfg.get(section,'url')))
		# 检查下载地址
		m3u_url = []
		html_url = []
		http_url = []
		for url in urls:	# url分类
			parsed_result = urlsplit(url)
			file = os.path.split(parsed_result.path)[1]
			if file == 'index.m3u8':
				m3u_url.append(url)
			elif 'html' in file:
				html_url.append(url)
			else:
				http_url.append(url)
		# 检查下载任务
		result = self.check_tasks(m3u_url,http_url,html_url)
		if result:
			asyncio.run_coroutine_threadsafe(self.rate(),self.new_loop)	# 总进度
		else:
			asyncio.run_coroutine_threadsafe(self.stop_loop(),self.new_loop)	# 关闭session和事件循环
		return result

	def check_tasks(self,m3u,http,html):
		'''检查下载任务'''
		result = None
		for url in m3u:
			queue,section = self.producer_m3u(url)
			if section:	# section存在时才会更新下载
				asyncio.run_coroutine_threadsafe(self.distribute_task(queue,section,self.queue_task,self.semaphore),self.new_loop)
				result = True
		for url in http:
			queue,section = self.producer_http(url)
			if section:
				asyncio.run_coroutine_threadsafe(self.distribute_task(queue,section,self.queue_task,self.semaphore),self.new_loop)
				result = True
		for url in html:
			pass
		return result

	async def distribute_task(self,queue,section,q,semaphore):
		'''将任务放在子线程中分发,并等待任务完成'''
		print('task-start: %s, run in tid: %s '%(section,threading.currentThread().ident))
		if queue and queue.qsize():	# 如果有队列任务则创建，没有则更新下载状态
			sub_workers = [asyncio.ensure_future(self.consumer(queue,semaphore)) for _ in range(self.max_tasks)]
			#在协程内等待结果. 通过await 来交出控制权, 同时等待tasks完成
			task_done,task_pending = await asyncio.wait(sub_workers)
		
		# 更新下载状态
		# 下载完成写入文件
		self.cfg.set(section,'success',"1")
		self.cfg.write(open(self.conf,'w'))

		# 任务结束，发送消息
		done = self.download_progress[section]['done']
		total = self.download_progress[section]['total']
		q.put('\ntask-end: {} | ({}/{}) {:.2%}'.format(section,done,total,done/total))

		# 删除已下载结束的任务
		self.download_progress.pop(section)

		# 合并文件,合并完成写入文件
		if done == total:	# 下载完成
			q.put('Merge Files...')
			file = await self.convert_m3u(section)
			self.cfg.set(section,'success',"2")
			self.cfg.write(open(self.conf,'w'))	# 合并完成写入文件
			q.put('\nDownload complete：%s'%file)
		else:
			q.put('Download Fail ! download_length: %s Content_Length: %s'%(done,total))

		# 如果都下载完了，则设置为None
		if self.download_progress == {}:
			self.download_progress = None

	async def consumer(self,queue,semaphore):
		'''接收下载任务'''
		async with semaphore:		#这里进行执行asyncio.Semaphore，
			while True:
				if queue.empty():
					return
				try:
					key = await queue.get()	# 获取下载任务
					status,key = await self.download(key)
					if status:
						self.done_chunk +=1		# 下载总完成kuai数增加
						self.download_progress[key['section']]['done'] += 1	#当前文件下载块数增加
					else:
						self.download_progress[key['section']]['fail'] +=1	# 失败数+1
						if self.download_progress[key['section']]['fail'] < self.download_progress[key['section']]['done']:
							queue.put_nowait(key)
				except asyncio.CancelledError:
					raise
				except Exception as e:
					pass

	async def download(self,key):
		'''下载文件'''
		# print(key)
		if key['ftype'] == 'm3u8':
			file_name = key['file_name']
			file_path = key['save_path'] + '/' + key['file_name']
			url = key['base_url'] + '/' + file_name
			for i in range(self.max_tries):
				try:
					async with self.session.get(url,timeout=5) as resp:
						async with aiofiles.open(file_path,'wb') as fd:
							await fd.write(await resp.read())
					self.done_size += os.path.getsize(file_path)	 #下载字节数增加
					self.download_progress[key['section']]['done_length'] += os.path.getsize(file_path)
					return True,key
				except asyncio.CancelledError:
					print('error')
					break
				except Exception as e:
					# 会有下载失败的情况
					pass
			return False,key
			
		elif key['ftype'] == 'http':
			file_name = key['file_name']
			file_path = key['save_path'] + '/' + key['file_name']
			url = key['url']

			while key['Amount'] < self.max_tries:
				try:
					headers = {'Range': 'bytes=%s-%s'%(key['begin'],key['end'])}
					async with self.session.get(url,headers=headers,timeout=5) as resp:
						async with aiofiles.open(file_path,'ab') as fd:
							while True:
								data = await resp.content.read(20480) 	#每次最多寫入20k
								status = resp.status
								if status not in (200,206):
									key['Amount'] +=1
									break
								if not data:
									break
								await fd.write(data)
								self.done_size += len(data)	# 下载总字节数增加
								self.download_progress[key['section']]['done_length'] += len(data)	#下载字节数增加
								
				except asyncio.CancelledError:
					# raise
					break
				except Exception as e:
					# 会有下载失败的情况
					# 下载失败，表示这块下载到一半就失败了，那么剩下的一半可以修改请求头，循环继续请求
					# print('块下载失败')

					if os.path.exists(file_path):
						tmp_size = os.path.getsize(file_path)
						if tmp_size > key['begin'] - key['start']:		# 表示又下载了部分
							key['begin'] = key['start'] + tmp_size	# 更新key['begin']
						else:
							key['Amount'] +=1
							
				if os.path.exists(file_path) and os.path.getsize(file_path)>=key['size']:
					return True,key
			key['Amount'] = 0	# 重置失败次数
			return False,key

	async def convert_m3u(self,section):
		'''文件合并'''
		# 从m3u8文件获取小文件
		path = self.cfg.get(section,'save_path')
		name = self.cfg.get(section,'name')
		download_file = '/'.join(path.split('/')[:-1]) + '/' + name
		m3u_list = []
		for root,dirs,files in os.walk(path):
			for file in files:
				if file.lower().endswith('.m3u8'):
					m3u = root +'/' + file
					m3u_list.append(m3u)
		# 如果不是m3u文件
		if not m3u_list:
			if os.path.exists(download_file):
				os.remove(download_file)

			try:
				async with aiofiles.open(download_file,'ab') as df:
					files = os.listdir(path)
					files_list = []
					for file in files:
						files_list.append(int(file))
					for file in sorted(files_list):
						file = path + '/' + str(file)
						# print(file)
						async with aiofiles.open(file,'rb') as f:
							await df.write(await f.read())
			except:
				raise
		for file in m3u_list:  #当前目录下的所有m3u8文件
			m3u_path,m3u_name = os.path.split(file)
			filename = m3u_path.replace('\\','/').split('/')[-1]
			download_file = '/'.join(m3u_path.split('/')[:-1])+'/'+filename+'.mp4'
			ts_list = []
			if os.path.exists(download_file):
				os.remove(download_file)
			async with aiofiles.open(file,'r') as f:
				lines = await f.readlines()
				for line in lines:
					if line[0] != '#' and line[0] != '':
						ts_name = line.strip().split('/')[-1]
						ts_file = m3u_path+'/'+ts_name
						if os.path.exists(ts_file) and os.path.getsize(ts_file)>0:
							ts_list.append(ts_file) #获取所有的ts文件
			if ts_list:
				try:
					async with aiofiles.open(download_file,'ab+') as video:  #合并文件
						for ts in ts_list:
							async with aiofiles.open(ts,'rb') as f:
								await video.write(await f.read()) 
				except Exception as e:
					raise
		# 删除缓存文件
		for root,dirs,files in os.walk(path):
			for file in files:
				os.remove(os.path.join(root,file))
			for d in dirs:
				os.rmdir(os.path.join(root,d))
		os.rmdir(path)
		return download_file

	async def rate(self):
		'''下载进度统计'''
		# widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker('#')),' ',SimpleProgress(),
		#    ' ', ETA(), ' ', FileTransferSpeed()]
		# pbar = ProgressBar(widgets=widgets, maxval=self.total).start()
		pbar = ShowProcess(self.total_chunk)
		while True:
			if not self.total_chunk:
				return
			if self.download_progress != {} and self.download_progress != None:
				rlt = pbar.show_process(self.done_chunk,self.done_size)
				# pbar.update(self.done_chunk)
				await asyncio.sleep(0.5)
				continue
			pbar.finish()
			return

	def producer_http(self,url):
		'''检查要下载的网络文件'''
		requests.packages.urllib3.disable_warnings()
		r = self.rs.get(url,stream=True,verify=False)
		url = r.url
		# print(r.headers)
		# print(r.status_code)
		
		# 先判断是否支持断点续传
		try:
			Content_Length = int(r.headers['Content-Length'])
			Accept_Ranges = r.headers['Accept-Ranges']
		except Exception as e:
			Accept_Ranges = ''

		if not Accept_Ranges:
			# 不支持断点续传,再接口请求试一下
			try:
				headers = {'Range': 'bytes=0-10240'} 
				r = self.rs.get(url,headers=headers,stream=True,verify=False)
			except:		# 仍然请求失败
				print('不支持断点续传')
				return False,None

		# 文件保存路径
		parsed_result = urlsplit(url)
		scheme = parsed_result.scheme
		netloc = parsed_result.netloc
		netpath = os.path.split(parsed_result.path)[0]
		file_name = os.path.split(parsed_result.path)[1]


		# 判断文件是否记录
		section = None
		for tmp_section in self.cfg.sections():
			if file_name in self.cfg.get(tmp_section,'name') and url == unquote(self.cfg.get(tmp_section,'url')):	# 文件已记录
				section = tmp_section
				url = unquote(self.cfg.get(tmp_section,'url'))
				Content_Length = int(self.cfg.get(tmp_section,'Content_Length'))
				save_path = self.cfg.get(tmp_section,'save_path')
				per_size = int(self.cfg.get(tmp_section,'per_size'))	# 一定要使用该值，不能每次计算
				break

		# 未记录，拆解文件
		if not section:
			# 支持断点续传方法一
			if Content_Length <= 1024*1024*200:	# 小于200M
				per_size = round(Content_Length/self.max_tasks)
			elif Content_Length <= 1024*1024*1024: 	# 小于1G
				per_size = 1024*1024*5
			else:
				per_size = 1024*1024*10

			# 保存到记录
			time_stamp = str(round(time.time()*1000))
			section = time_stamp 	# 保存到队列用
			save_path = download_dir+'/'+time_stamp
			self.cfg.add_section(time_stamp)
			self.cfg.set(time_stamp,'success',str(0))
			self.cfg.set(time_stamp,'total',str(0))
			# self.cfg.set(time_stamp,'done',str(0))
			self.cfg.set(time_stamp,'url',quote(url))
			self.cfg.set(time_stamp,'protocol',scheme)
			self.cfg.set(time_stamp,'name',file_name)
			self.cfg.set(time_stamp,'Content_Length',str(Content_Length))
			self.cfg.set(time_stamp,'Accept_Ranges',Accept_Ranges)
			self.cfg.set(time_stamp,'save_path',save_path)
			self.cfg.set(time_stamp,'per_size',str(per_size))
			# self.cfg.write(open(self.conf,'w'))

		# 判断是否创建文件夹
		if not os.path.exists(save_path):	#创建目录
			os.makedirs(save_path,exist_ok=True)

		# 保存到队列
		done = 0			# 保存已经下载的块数
		done_length = 0		# 保存已经下载的字节数
		start_byte = 0
		queue = asyncio.Queue(loop=self.new_loop)
		while start_byte<Content_Length:
			# 获取片段
			tmp_start = start_byte	# 块的开始字节
			tmp_begin = tmp_start	# 块从这个字节开始下载
			next_byte = start_byte+per_size

			if round((Content_Length-next_byte+1)/per_size):
				tmp_end = start_byte + per_size -1
				tmp_size = per_size
			else:
				tmp_end = ''
				tmp_size = Content_Length - start_byte
				next_byte = Content_Length
			# 文件存在且下载完成时
			chunk_file = save_path+'/'+str(start_byte)
			if os.path.exists(chunk_file) and os.path.getsize(chunk_file)>=tmp_size:	#要大于
				done += 1
				done_length += os.path.getsize(chunk_file)
				start_byte = next_byte
				continue
			# 文件存在但未下载完成时
			if os.path.exists(chunk_file):	# 如果未下载完成，则修改请求头继续下载
				tmp_done = os.path.getsize(chunk_file)
				done_length += tmp_done
				tmp_begin = tmp_start + tmp_done	# 修改下载起点
			queue.put_nowait({'section':section,'ftype':'http','file_name':str(start_byte),
				'save_path':save_path,'url':url,'start':tmp_start,'end':tmp_end,
				'begin':tmp_begin,'size':tmp_size,'Amount':0})	# Amount用来保存下载失败次数
			start_byte = next_byte
		total = done+queue.qsize()	# 总块数
		self.cfg.set(section,'total',str(total))
		self.cfg.set(section,'done',str(done))
		self.cfg.set(section,'done_length',str(done_length))
		self.cfg.write(open(self.conf,'w'))
		self.download_progress[section] = {'total':total,'done':done,'fail':0,'content_length':Content_Length,'done_length':done_length}
		self.total_chunk += total
		self.done_chunk += done
		self.total_size += Content_Length
		self.done_size += done_length
		return queue,section
	
	def producer_m3u(self,url):
		'''检查要下载的网络文件'''
		queue = asyncio.Queue(loop=self.new_loop)
		parsed_result = urlsplit(url)
		scheme = parsed_result.scheme
		netloc = parsed_result.netloc
		netpath = os.path.split(parsed_result.path)[0]
		file_name = os.path.split(parsed_result.path)[1]
		total = 0	# 文件数量
		done = 0 	# 完成数量
		save_path = download_dir + '/' + netpath.replace('/','')
		index_file = False
		# 判断是否记录
		for section in self.cfg.sections():
			if save_path in self.cfg.get(section,'save_path'):	# 文件已记录
				index_file = True
				break
		if index_file and os.path.exists(save_path+'/'+file_name) and os.path.getsize(save_path+'/'+file_name)>1024*2:	# 引导文件已经下载
			with open(save_path+'/'+file_name,'r') as fd:
				for line in fd.readlines():
					if line and line[0] != '#':
						path,file = os.path.split(line.replace('\n',''))
						# print(path,file)
						# 判断是否已经下载了，若是小于5k也会重新下载
						if os.path.exists(save_path+'/'+file) and  os.path.getsize(save_path+'/'+file) > 5120:
							done +=1 # 完成数量+1
							continue
						if not path:	#只有文件名时
							base_url = scheme+'://'+netloc+netpath
						elif scheme in path:	#为完整url时
							base_url = path
						else:   #含有路径时
							base_url = scheme+'://'+netloc+ path
						queue.put_nowait({'section':section,'ftype':'m3u8','file_name':file,'save_path':save_path,'base_url':base_url})
		else:   # 下载文件		  
			if not os.path.exists(save_path):	#创建目录
				os.makedirs(save_path,exist_ok=True)
			for i in range(self.max_tries):
				try:
					r = self.rs.get(url,timeout=20)
					#先判断文件是否正确
					for t_url in r.text.split('\n'):
						if t_url != '' and t_url[0] !='#':
							p_rlt = urlsplit(t_url)
							if os.path.split(p_rlt.path)[-1] == 'index.m3u8':	#重新下载
								if not p_rlt.scheme:	# 不是完整url链接
									t_url = scheme+'://'+netloc+ t_url
								r = self.rs.get(t_url,timeout=20)
								break
							else:	# 文件正确，退出循环
								break

					# 保存文件
					with open(save_path+'/'+file_name,'wb') as fd:
						fd.write(r.content)

					# 记录到文件
					if not index_file: 
						time_stamp = str(round(time.time()*1000))
						section = time_stamp
						section = str(section)
						self.cfg.add_section(time_stamp)
						self.cfg.set(time_stamp,'success',str(0))
						self.cfg.set(time_stamp,'total',str(0))
						# self.cfg.set(time_stamp,'done',str(0))
						self.cfg.set(time_stamp,'url',quote(url))
						self.cfg.set(time_stamp,'protocol',scheme)
						self.cfg.set(time_stamp,'name',file_name)
						self.cfg.set(time_stamp,'Content_Length',str(0))
						self.cfg.set(time_stamp,'save_path',save_path)
						# self.cfg.write(open(self.conf,'w'))

					# 加入队列
					for data in r.text.split('\n'):
						if data != '' and data[0] !='#':
							path,file = os.path.split(data)
							if not path:	#只有文件名时
								base_url = scheme+'://'+netloc+netpath
							elif scheme in path:	#为完整url时
								base_url = path
							else:   #含有路径时
								base_url = scheme+'://'+netloc+ path
							queue.put_nowait({'section':section,'ftype':'m3u8','file_name':file,'save_path':save_path,'base_url':base_url})
					break
				except Exception as ce:
					# raise
					pass
			else:
				print('%-15s 下载失败'%save_path.split('/')[-1])
				return queue,None
		if not queue.qsize():
			print('%-15s 下载完毕'%save_path.split('/')[-1])

		total = done + queue.qsize()
		self.cfg.set(section,'total',str(total))
		self.cfg.set(section,'done',str(done))
		self.cfg.set(section,'done_length',str(0))
		self.cfg.write(open(self.conf,'w'))
		self.download_progress[section] = {'total':total,'done':done,'fail':0,'content_length':0,'done_length':0}
		self.total_chunk += total		 # 下载总数增加
		self.done_chunk += done 	# 初始化总完成数量
		self.total_size += 0
		self.done_size += 0
		return queue,section

	def get_html_task(self,urls):
		pass

	def run(self):
		result = self.check_url(self.urls)
		while result:
			try:
				x = self.queue_task.get(timeout=0.5)
				print('%s'%x) 
			except KeyboardInterrupt as e:
				print('KeyboardInterrupt')
				return
			except Exception as e:
				if self.download_progress == None:	# 任务完成
					return

class ShowProcess():
	"""
	显示处理进度的类
	调用该类相关函数即可实现处理进度的显示
	"""

	# 初始化函数，需要知道总共的处理次数
	def __init__(self, max_steps=0, infoDone = 'Done'):
		self.max_steps = max_steps
		self.processes = {}
		self.max_arrow = 50
		self.i = 0
		self.infoDone = infoDone
		self.size = 0
		self.speed = 0
		self.start_time = time.time()
		self.total = 0
		self.done = 0

	# 显示函数，根据当前的处理进度i显示进度
	# 效果为[>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>]100.00%
	def show_process(self, i=None,size=0):
		if self.i >= self.max_steps:
			return True
		end_time = time.time()
		if i is not None:
			self.i = i
		else:
			self.i += 1
		try:
			if size == self.size:
				return
			self.speed = (size - self.size)/(end_time - self.start_time)
		except:
			self.speed = 0
		speed_str = " Speed: %-12s"%self.format_size(self.speed)
		self.size = size
		self.start_time = end_time
		num_arrow = int(self.i * self.max_arrow / self.max_steps) #计算显示多少个'#'
		num_line = self.max_arrow - num_arrow #计算显示多少个' '
		percent = self.i * 100.0 / self.max_steps #计算完成进度，格式为xx.xx%
		process_bar = '[' + '#' * num_arrow + ' ' * num_line + ']' + ' %s/%s %7.2f%% ' %(self.i,self.max_steps,percent) + speed_str + '\r' #带输出的字符串，'\r'表示不换行回到最左边
		sys.stdout.write(process_bar) #这两句打印字符到终端
		sys.stdout.flush()
		
	def show_processes(self,d):
		end_time = time.time()
		total_size = 0
		done_size = 0
		for section,value in d.items():
			total_size += value['content_length']
			done_size += value['done_length']
		try:
			speed = (done_size - self.done)/(end_time - self.start_time)
		except:
			speed = 0
		speed_str = " Speed: %-12s"%self.format_size(speed)
		num_arrow = int(done_size * self.max_arrow / total_size) #计算显示多少个'#'
		num_line = self.max_arrow - num_arrow #计算显示多少个' '
		percent = done_size * 100.0 / total_size #计算完成进度，格式为xx.xx%
		process_bar = '[' + '#' * num_arrow + ' ' * num_line + ']' + ' %s/%s %7.2f%% ' %(done_size,total_size,percent) + speed_str	+'\r' # + '\r' #带输出的字符串，'\r'表示不换行回到最左边

		self.total = total_size
		self.done = done_size
		self.start_time = end_time
		sys.stdout.write(process_bar)
		sys.stdout.flush()
		if done_size >= total_size:
			return self.finish()
	
	def format_size(self,b):
		try:
			b = float(b)
			kb = b / 1024
		except:
			return "Error"
		if kb>=1024:
			M = kb / 1024
			if M>=1024:
				G = M / 1024
				return "%.2fGb/s"%G
			else:
				return "%.2fMb/s"%M
		else:
			return "%.2fKb/s"%kb

	def finish(self):
		# print('\n')
		# print(self.infoDone)
		self.i = 0
		return True

if __name__ == '__main__':
	Dl = Download(urls=sys.argv[1:])