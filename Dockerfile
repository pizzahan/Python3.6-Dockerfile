FROM daocloud.io/centos:7

# Install Python 3.6
RUN yum -y install https://centos7.iuscommunity.org/ius-release.rpm && \
    yum -y install python36u \
                   python36u-pip \
                   python36u-devel \
    # clean up cache
    && yum -y clean all

# App home
RUN mkdir -p /app
WORKDIR /app
EXPOSE 80
CMD ["bash"]
ADD downloaderdistribute /app/
RUN pip3.6 install redis
RUN pip3.6 install pymysql
RUN pip3.6 install requests
RUN pip3.6 install kafka-python
RUN python3.6 gain_downloader_main.py&
#解决中文文件名报错问题 UnicodeEncodeError: 'ascii' codec can't encode characters in position 0-3
RUN export LANG=en_US.UTF-8
RUN set -o vi
