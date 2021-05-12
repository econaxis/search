SHELL:=bash
ifdef DATA_FILES_DIR
    data-file-path := $(DATA_FILES_DIR)
else
    data-file-path :=/mnt/nfs/.cache/data-files
endif

RSYNC_COMMAND:=rsync -avh --filter=':- .gitignore' --info=progress2

clear:
	cd $(data-file-path) && \
    $(RSYNC_COMMAND) $(data-file-path)/processed/ $(data-file-path)/data/ && \
	(cd data&& fd . > ../total-files-list)
	python source/tarrer.py $(data-file-path)

clear-all: clear
	fd . $(data-file-path)/indices -x rm -r {}

transferall: transfer-data-files transfer

transfer-data-files:
	$(RSYNC_COMMAND) $(data-file-path)/ henry-80q7:$(data-file-path);

transfer:
	$(RSYNC_COMMAND) . henry@henry-80q7:~/search;
transfer-rev:
	$(RSYNC_COMMAND) henry@henry-x1:~/search/ ~/search/;

build-debug:
	cmake --build cmake-build-debug -j 4

build:
	cmake --build cmake-build-release -j 4

remake:
	(cd cmake-build-release && rm -rf * && cmake -G Ninja .. && cmake --build . -j 4)

index:
	cmake-build-release/search

search:
	cmake-build-release/search 1
