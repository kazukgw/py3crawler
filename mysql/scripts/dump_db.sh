mysqldump -uroot -p --all-databases | gzip -9 > /root/backup/dump_$(date +'%Y%m%d_%H%M%S').sql.gz
