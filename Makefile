data-file-path = /home/henry/.cache/data-files

clear:
	fd . $(data-file-path)/processed -x mv {} $(data-file-path)/ && echo {} a;

clear-all: clear
	fd . $(data-file-path)/indices -x trash {}
