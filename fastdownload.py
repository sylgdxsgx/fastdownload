#-- 批量下载网络文件，若是下载失败，则再次运行继续下载
#-- 批量下载前，要能组合出所有文件的下载url
#--put中加入done来处理
#--get改为get_nowait

import os,asyncio,aiohttp,aiofiles

from tqdm import tqdm
 
url = 'https://56.com-t-56.com/20190721/23773_0455e097/1000k/hls/index.m3u8'
url = 'https://youku.cdn-tudou.com/20180617/6445_b36f02c1/1000k/hls/index.m3u8'
url = 'https://iqiyi.com-1-iqiyi.com/20190820/5678_6c165f3e/1000k/hls/index.m3u8'
headers = {}
os.makedirs('download',exist_ok=True)
fail_file = 'fail.txt'

class Spider(object):

    def __init__(self,m3u8_url,max_tasks=10,download_task=50):

        self.loop = asyncio.get_event_loop()
        self.max_tries = 3  #每个文件重试次数
        self.max_tasks = max_tasks  #请求进程数
        self.download_task = download_task #文件下载队列数
        self.key_queue = asyncio.Queue(loop=self.loop)  #
        self.download_queue = asyncio.Queue(loop=self.loop)
        self.session = aiohttp.ClientSession(loop=self.loop)
        hostname = m3u8_url.split('/')
        m3u8_url = hostname[-1]
        hostname = '/'.join(hostname[:-1])
        self.key_queue.put_nowait(
            {'file_name':m3u8_url,'save_path':'download','hostname':hostname})
        self.key_done = False
        self.download_done = False
        self.total = 0
        self.fail = []

    def close(self):
        '''回收session'''
        self.session.close()

    def save_fail(self):
        if self.fail:
            print('下载失败！请重新执行下载')
            with open(fail_file,'w') as f:
                for line in self.fail:
                    f.write(str(line)+'\r')  #失败文件写入文件中
        else:
            if os.path.exists(fail_file):
                os.remove(fail_file)

    def download_fail(self,file):
        if os.path.exists(file):
            print('>开始继续下载')
            with open(file,'r') as f:
                lines = f.readlines()
                if not lines:
                    return 0
                for line in lines:
                    self.download_queue.put_nowait(eval(line))
            return 1
        return 0

    async def work(self):

        try:
            while True:
                url = await self.key_queue.get_nowait()
                await self.handle(url)
                self.key_queue.task_done()
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            print('退出协程')
            pass
        except Exception as e:
            pass

    async def download_work(self):
        try:
            while True:
                url = await self.download_queue.get_nowait()
                await self.download_handle(url)
                self.download_queue.task_done()
                await asyncio.sleep(1)
                print(self.download_done)
        except asyncio.CancelledError as ce:
            pass
            print(ce)
        except Exception as e:
            self.total +=1
            print('退出协程 %d'%self.total)

            pass
            # print(e)

    async def handle(self,key):
        '''处理请求'''
        file_name = key['file_name']
        url = key['hostname'] + '/' + key['file_name']
        path = key['save_path'] + '/' + key['file_name']
        hostname = key['hostname']

        for i in range(self.max_tasks):
            try:
                async with self.session.get(url,headers=headers,timeout=60) as resp:
                    async with aiofiles.open(path,'wb') as fd:
                        await fd.write(await resp.read())

                    async with aiofiles.open(path,'r') as fd:
                        for line in await fd.readlines():
                            if line[0] != '#':
                                self.download_queue.put_nowait(
                                    {'file_name':line.replace('\n',''),'save_path':key['save_path'],'hostname':hostname})
                print('下载文件 %s 成功'%file_name)
                break
            except aiohttp.ClientError as ce:
                pass
                print(ce)
                pass
            except Exception as e:
                pass
                print(e)
        else:
            print('下载文件 %s 失败'%file_name)

    async def download_handle(self,key):

        file_name = key['file_name']
        path = key['save_path'] + '/' + key['file_name']
        hostname = key['hostname']
        url = key['hostname'] + '/' + key['file_name']

        for i in range(self.max_tasks):
            try:
                async with self.session.get(url,timeout=30) as resp:
                    async with aiofiles.open(path,'wb') as fd:
                        await fd.write(await resp.read())
                print('下载完成：%s'%file_name)
                break
            except Exception as e:
                # print(e)
                pass
        else:
            self.fail.append(key)
            
    async def run(self):
        if not self.download_fail(fail_file):
            print('>下载网络资源')
            workers_key = [asyncio.Task(self.work(),loop=self.loop) for _ in range(self.max_tasks)]
        else:
            self.key_queue.task_done()  
        workers = [asyncio.Task(self.download_work(),loop=self.loop) for _ in range(self.download_task)]
        await self.key_queue.join() #阻塞 等待队列中任务全部处理完毕，配合queue.task_done使用
        await self.download_queue.join()
        self.key_done = True
        self.download_done = True
        # for w in asyncio.Task.all_tasks():
        #     w.cancel()
        self.save_fail()
        # return asyncio.Task.all_tasks()

if __name__ == '__main__':
    loop = asyncio.get_event_loop() 
    crawler = Spider(url,max_tasks=1,download_task=100)
    loop.run_until_complete(crawler.run())
    # crawler.close()
    loop.close()