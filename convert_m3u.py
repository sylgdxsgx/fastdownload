'''用法：python convert_m3u.py path'''
import sys,os
def convert_m3u(path):
    '''文件合并'''
    # 从m3u8文件获取小文件
    m3u_list = []
    for root,dirs,files in os.walk(path):
        for file in files:
            if file.lower().endswith('.m3u8'):
                m3u = root +'/' + file
                m3u_list.append(m3u)

    for file in m3u_list:  #当前目录下的所有m3u8文件
        m3u_path,m3u_name = os.path.split(file)
        filename = m3u_path.replace('\\','/').split('/')[-1]
        videofile = '/'.join(m3u_path.split('\\')[:-1])+'/'+filename+'.mp4'
        # videofile = m3u_path+'/'+filename+'.mp4'
        ts_list = []
        if os.path.exists(videofile):
            os.remove(videofile)
        with open(file,'r') as f:
            lines = f.readlines()
            for line in lines:
                if line[0] != '#' and line[0] != '':
                    ts_name = line.strip().split('/')[-1]
                    ts_file = m3u_path+'/'+ts_name
                    if os.path.exists(ts_file) and os.path.getsize(ts_file)>0:
                        ts_list.append(ts_file) #获取所有的ts文件
        if ts_list:
            try:
                with open(videofile,'ab+') as video:  #合并文件
                    for ts in ts_list:
                        with open(ts,'rb') as f:
                            video.write(f.read()) 
            except Exception as e:
                raise
    return videofile
    
if __name__ == "__main__":
    argv = sys.argv
    if len(argv)>1:
        print('转换完成:%s'%convert_m3u(argv[1]))