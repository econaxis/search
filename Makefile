clear:
	fd . data-files/processed -x mv {} data-files/

clear-all: clear
	fd . data-files/indices -x trash {}
