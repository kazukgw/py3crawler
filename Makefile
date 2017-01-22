backup_db_gz:
	docker-compose run --rm mysql tar czvf /backup/$$(date +'%Y%m%d_%H%M%S').gz -C /var/lib mysql

backup_db_volume:
	$(eval VOLNAME := $(shell echo py3crawler_mysql_data_$$(date +'%Y%m%d_%H%M%S')))
	docker volume create $(VOLNAME) && \
	docker run --rm -v py3crawler_mysql_data:/data_src \
		-v $(VOLNAME):/data_dst ubuntu cp -r -T /data_src /data_dst
