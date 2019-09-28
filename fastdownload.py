#-- 批量下载网络文件，若是下载失败，则再次运行继续下载

import os,asyncio,aiohttp,aiofiles,threading,time,requests,sys
from progressbar import *
from queue import Queue
from urllib.parse import urlsplit
 
urls = [
'https://youku.com-y-youku.com/20190816/6287_65cb2004/1000k/hls/index.m3u8',
]

download_dir = r'C:\Users\Administrator\AppData\Roaming\myDownload'
os.makedirs(download_dir,exist_ok=True)

class Download(object):
    def __init__(self,loop,urls,max_tasks=32):
        self.urls = urls
        self.headers = {}
        self.max_tasks = max_tasks
        self.max_tries = 3
        self.new_loop = asyncio.new_event_loop()
        self.loop = loop
        self.session = aiohttp.ClientSession(loop=self.new_loop)
        self.queue_done = asyncio.Queue(loop=self.new_loop)
        self.total = 0  #下载总数
        self.done = 0   #下载完成数
        self.files = []  #保存要下载的文件
        self.success_count = {} #每个大文件中，小文件的完成计数
        self.total_size = 0 #文件总大小

    def start_thread(self):
        t = threading.Thread(target=self.start_loop,args=(self.new_loop,))
        t.start()

    def start_loop(self,loop):
        asyncio.set_event_loop(loop)
        loop.run_forever()

    def check_url(self,url):
        '''返回队列和文件名或文件路径'''
        parsed_result = urlsplit(url)
        if parsed_result.path.split('/')[-1] == 'index.m3u8':
            queue,fpath = self.producer_m3u8(url)
        else:
            print('无法判断下载文件')
            return False,None
        return queue,fpath

    async def stop_loop(self):
        await self.session.close()
        self.new_loop.stop()

    async def convert_m3u8(self,path):
        '''文件合并'''
        # 从m3u8文件获取小文件
        m3u8_list = []
        for root,dirs,files in os.walk(path):
            for file in files:
                if file.lower().endswith('.m3u8'):
                    m3u8 = root +'/' + file
                    m3u8_list.append(m3u8)
        for file in m3u8_list:  #当前目录下的所有m3u8文件
            m3u8_path,m3u8_name = os.path.split(file)
            filename = m3u8_path.split('/')[-1]
            videofile = m3u8_path+'/'+filename+'.mp4'
            ts_list = []
            if os.path.exists(videofile):
                os.remove(videofile)
            async with aiofiles.open(file,'r') as f:
                lines = await f.readlines()
                for line in lines:
                    if line[0] != '#' and line[0] != '':
                        ts_name = line.strip().split('/')[-1]
                        ts_file = m3u8_path+'/'+ts_name
                        if os.path.exists(ts_file) and os.path.getsize(ts_file)>0:
                            ts_list.append(ts_file) #获取所有的ts文件
            if ts_list:
                try:
                    async with aiofiles.open(videofile,'ab+') as video:  #合并文件
                        for ts in ts_list:
                            async with aiofiles.open(ts,'rb') as f:
                                await video.write(await f.read()) 
                except Exception as e:
                    raise
        return videofile

    def producer_http(self,url,save_path):
        r = requests.get(url,stream=True,verify=False)
        # print(r.headers)
        
        # 先判断是否支持断点续传
        try:
            Content_Length = int(r.headers['Content-Length'])
            Accept_Ranges = r.headers['Accept-Ranges']

            if r.status_code != 206:
                Accept_Ranges = None
        except Exception as e:
            Accept_Ranges = None

        print(Accept_Ranges)
        if not Accept_Ranges:
            # 不支持断点续传
            return False

        # 支持断点续传方法一
        per_byte = round(Content_Length/self.max_tasks)
        start_byte = 0
        while start_byte<Content_Length:
            next_byte = start_byte+per_byte
            if next_byte < Content_Length:
                if round((Content_Length-next_byte+1)/per_byte):
                    tmp_start = start_byte
                    tmp_end = next_byte-1
                    tmp_size = next_byte - start_byte
                else:
                    tmp_start = start_byte
                    tmp_end = ''
                    tmp_size = Content_Length - start_byte +1
                    next_byte = Content_Length
            else:
                tmp_start = start_byte
                tmp_end = ''
                tmp_size = Content_Length - start_byte +1

            self.queue.put_nowait({'ftype':'bigfile','file_name':str(start_byte),'save_path':save_path,'url':url,'start':tmp_start,'end':tmp_end,'size':tmp_size,'Amount':0})
            start_byte = next_byte
        # print(self.queue.qsize())
        # 支持断点续传方法二

    def producer_m3u8(self,url):
        queue = asyncio.Queue(loop=self.new_loop)
        parsed_result = urlsplit(url)
        scheme = parsed_result.scheme
        netloc = parsed_result.netloc
        netpath = os.path.split(parsed_result.path)[0]
        file_name = parsed_result.path.split('/')[-1]
        try:
            save_path = download_dir+'/'+parsed_result.path.split('/')[-4] if parsed_result.path.split('/')[-4] else file_name
        except:
            save_path = download_dir+'/'+file_name
        # 判断引导文件是否已经下载了
        if os.path.exists(save_path+'/'+file_name):
            with open(save_path+'/'+file_name,'r') as fd:
                for line in fd.readlines():
                    if line and line[0] != '#':
                        path,file = os.path.split(line.replace('\n',''))
                        # print(path,file)
                        # 判断是否已经下载了，若是小于5k也会重新下载
                        if os.path.exists(save_path+'/'+file) and  os.path.getsize(save_path+'/'+file) > 5120:
                            continue
                        if not path:    #只有文件名时
                            base_url = scheme+'://'+netloc+netpath
                        elif scheme in path:    #为完整url时
                            base_url = path
                        elif netpath in path:   #含有路径时
                            base_url = scheme+'://'+netloc+ path
                        queue.put_nowait({'ftype':'m3u8','file_name':file,'save_path':save_path,'base_url':base_url})
        else:   # 下载文件          
            if not os.path.exists(save_path):
                os.makedirs(save_path,exist_ok=True)
            for i in range(self.max_tries):
                try:
                    r = requests.get(url,timeout=20)
                    with open(save_path+'/'+file_name,'wb') as fd:
                        fd.write(r.content)

                    for data in r.text.split('\n'):
                        if data != '' and data[0] !='#':
                            path,file = os.path.split(data)
                            if not path:    #只有文件名时
                                base_url = scheme+'://'+netloc+netpath
                            elif scheme in path:    #为完整url时
                                base_url = path
                            elif netpath in path:   #含有路径时
                                base_url = scheme+'://'+netloc+ path
                            queue.put_nowait({'ftype':'m3u8','file_name':file,'save_path':save_path,'base_url':base_url})
                    break
                except Exception as ce:
                    pass
            else:
                print('%-15s 下载失败'%save_path.split('/')[-1])
                return queue,save_path
        if not queue.qsize():
            print('%-15s 下载完毕'%save_path.split('/')[-1])
        else:
            print('%-15s 准备下载...'%save_path.split('/')[-1])
            self.files.append(save_path)
        self.total += queue.qsize()         # 下载总数增加
        self.success_count[save_path] = 0   # 初始化下载成功数量
        return queue,save_path

    async def consumer(self,queue):
        while True:
            if queue.empty():
                return True
            try:
                key = await queue.get()
                rlt = await self.download(key)
                if not rlt[0]:
                    pass
                else:
                    self.success_count['/'.join(rlt[1].split('/')[:-1])] +=1
                    self.done +=1
                    self.total_size +=int(rlt[0])
                queue.task_done()   # 用来触发check_done
            except asyncio.CancelledError:
                raise
            except Exception as e:
                pass

    async def download(self,key):
        '''返回文件大小和文件路径'''
        # print(key)
        if key['ftype'] == 'm3u8':
            file_name = key['file_name']
            file_path = key['save_path'] + '/' + key['file_name']
            url = key['base_url'] + '/' + file_name
            # print(url)
            for i in range(self.max_tries):
                try:
                    async with self.session.get(url,timeout=10) as resp:
                        async with aiofiles.open(file_path,'wb') as fd:
                            await fd.write(await resp.read())
                            # while True:
                            #     chunk = await resp.content.read(10240)  #10k
                            #     if not chunk:
                            #         break
                            #     await fd.write(chunk)
                            #     self.total_size +=chunk     #下载字节数增加
                        # 获取文件大小
                        try:
                            Content_Length = resp.headers['Content-Length']
                        except:
                            Content_Length = os.path.getsize(file_path)
                    return Content_Length, file_path
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    pass
            else:
                return False,file_path
        elif key['ftype'] == 'bigfile':
            file_name = key['file_name']
            file_path = key['save_path'] + '/' + key['file_name']
            url = key['url']
            headers = {'Range': 'bytes=%s-%s'%(key['start'],key['end'])} 

            for i in range(self.max_tries):
                # 先判断文件是否下载了,并且更新头信息
                if os.path.exists(file_path):
                    tmp_size = os.path.getsize(file_path)
                    if tmp_size == key['size']:
                        return True,file_path
                    headers = {'Range': 'bytes=%s-%s'%(key['start']+tmp_size,key['end'])}

                # headers={'Range': 'bytes=0-10239'}
                try:
                    async with self.session.get(url,headers=headers,timeout=30) as resp:
                        async with aiofiles.open(file_path,'ab') as fd:
                            await fd.write(await resp.read())
                    self.done +=1
                    return True,file_path
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    pass
            else:
                return False,file_path

    async def check_done(self,queue,path,size):
        '''下载完成发消息'''
        # 下载进度条
        # pbar = ShowProcess(size)
        # while True:
        #     pbar.show_process(self.success_count[path])
        #     if queue.empty():   # 如果为空则表示下载结束
        #         break
        #     await asyncio.sleep(0.5)
        try:
            await queue.join()
            msg = '\r任务结束：{:<15s} | ({}/{}) {:.2%}'.format(path.split('/')[-1],self.success_count[path],size,self.success_count[path]/size)
            self.queue_done.put_nowait(msg)
            if self.success_count[path] == size:
                self.queue_done.put_nowait('合并文件...')
                file = await self.convert_m3u8(path)
                self.queue_done.put_nowait('生成文件：%s'%file)
            else:
                self.queue_done.put_nowait('未下载完成')
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        self.files.remove(path) #删除已下载结束的文件

    async def rate(self):
        '''下载进度统计'''
        widgets = ['Progress: ', Percentage(), ' ', Bar(marker=RotatingMarker('#')),' ',SimpleProgress(),
           ' ', ETA(), ' ', FileTransferSpeed()]
        # pbar = ProgressBar(widgets=widgets, maxval=self.total).start()
        pbar = ShowProcess(self.total)
        try:
            while True:
                if not self.total:
                    break
                pbar.show_process(self.done,self.total_size)
                # pbar.update(self.done)
                if self.files:
                    await asyncio.sleep(1)
                    continue
                # pbar.finish()
                # await asyncio.sleep(10)
                # 序列为空则说明文件下载完毕了，输出文件下载情况
                for _ in range(self.queue_done.qsize()):
                    print(await self.queue_done.get())
                return
        except asyncio.CancelledError:
            raise
        except Exception:
            pass

    def get_result(self,future):
        result = future.result()
        print(result)

    def run(self):
        sub_workers = []
        self.start_thread()
        for url in self.urls:
            queue,fpath = self.check_url(url)
            if queue and queue.qsize():
                size = queue.qsize()
                sub_workers += [asyncio.run_coroutine_threadsafe(self.consumer(queue),self.new_loop) for _ in range(self.max_tasks)]
                # for _ in range(self.max_tasks):
                #     task = asyncio.run_coroutine_threadsafe(self.consumer(queue),self.new_loop)
                #     task.add_done_callback(self.get_result)
                #     sub_workers.append(task)
                sub_workers.append(asyncio.run_coroutine_threadsafe(self.check_done(queue,fpath,size),self.new_loop))
        
        sub_workers.append(asyncio.run_coroutine_threadsafe(self.rate(),self.new_loop)) # 总进度
        sub_workers = [asyncio.wrap_future(worker,loop=self.loop) for worker in sub_workers]
        try:
            self.loop.run_until_complete(asyncio.wait(sub_workers)) #等待协程执行完成
            # 因为协程已经自动关闭了，所以不用手动关闭协程了
            # for worker in sub_workers:
            #     print(worker)
                # worker.cancel()
        except asyncio.CancelledError:
            print('CancelledError shuc')
            raise
        except KeyboardInterrupt as e:
            raise
            # for worker in sub_workers:
            #     worker.cancel()
            print(e)
        except Exception as e:
            print(e)

        # 关闭session和事件循环,已经主线程的事件循环
        asyncio.run_coroutine_threadsafe(self.stop_loop(),self.new_loop)
        self.loop.close()

class ShowProcess():
    """
    显示处理进度的类
    调用该类相关函数即可实现处理进度的显示
    """

    # 初始化函数，需要知道总共的处理次数
    def __init__(self, max_steps=0, infoDone = 'Done'):
        self.max_steps = max_steps
        self.max_arrow = 50
        self.i = 0
        self.infoDone = infoDone
        self.size = 0
        self.start_time = time.time()

    # 显示函数，根据当前的处理进度i显示进度
    # 效果为[>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>]100.00%
    def show_process(self, i=None,size=0):
        if self.i >= self.max_steps:
            # self.finish()
            return
        end_time = time.time()
        if i is not None:
            self.i = i
        else:
            self.i += 1
        try:
            speed = (size - self.size)/(end_time - self.start_time)
        except:
            speed = 0
        speed_str = " Speed: %-10s"%self.format_size(speed)
        self.size = size
        self.start_time = end_time
        num_arrow = int(self.i * self.max_arrow / self.max_steps) #计算显示多少个'#'
        num_line = self.max_arrow - num_arrow #计算显示多少个' '
        percent = self.i * 100.0 / self.max_steps #计算完成进度，格式为xx.xx%
        process_bar = '[' + '#' * num_arrow + ' ' * num_line + ']' + ' %s/%s %7.2f%% ' %(self.i,self.max_steps,percent) + speed_str + '\r' #带输出的字符串，'\r'表示不换行回到最左边
        sys.stdout.write(process_bar) #这两句打印字符到终端
        sys.stdout.flush()
        
    
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
        print('')
        # print(self.infoDone)
        self.i = 0


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    Dl = Download(loop=loop,urls=urls)
    Dl.run()