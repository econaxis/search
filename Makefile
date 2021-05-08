ifdef DATA_FILES_DIR
    data-file-path := $(DATA_FILES_DIR)
else
    data-file-path :=/mnt/henry-80q7/.cache/data-files
endif

RSYNC_COMMAND:=rsync -avh --filter=':- .gitignore' --info=progress2

clear:
	cd $(data-file-path) && \
	fd -p '$(data-file-path)/processed/' -x mv {} $(data-file-path)/data/  && \
	(cd data&& fd . > ../total-files-list)


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
