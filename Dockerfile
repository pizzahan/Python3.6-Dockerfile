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
ADD crawler /app/
ENV redis_host 127.0.0.1
ENV redis_port 21601
ENV redis_password Mindata123

ENV download_run_seqs 20000100
ENV download_stop_key event_qichacha_downloader
ENV download_data_path ./data
ENV download_stat_hash gain_stat_hash
ENV download_proxy_key abu_pro_proxy
ENV download_proxy_index qichacha_proxy_idx
ENV download_lock_sesseion_sec 1
ENV download_time_out 5

ENV mysql_host 172.26.249.246
ENV mysql_port 3306
ENV mysql_user md
ENV mysql_password maida6868
ENV mysql_db crawler
