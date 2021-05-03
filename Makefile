ifdef DATA_FILES_DIR
    data-file-path := $(DATA_FILES_DIR)
else
    data-file-path :=/mnt/henry-80q7/.cache/data-files
endif

clear:
	cd $(data-file-path) && \
	fd -p '$(data-file-path)/processed.*/' -x mv {} $(data-file-path)/  && \
	fd -p -t directory '$(data-file-path)/processed.*\S' -X rm -r {};


clear-all: clear
	fd . $(data-file-path)/indices -x rm -r {}

transferall: transfer-data-files transfer

transfer-data-files:
	rsync $(data-file-path)/ henry-80q7:$(data-file-path) -a --info=progress2;

transfer:
	rsync . henry@henry-80q7:~/search -a --info=progress2;

build-debug:
	cmake --build cmake-build-debug -j 4

build:
	cmake --build cmake-build-release -j 4


index:
	cmake-build-release/search

search:
	cmake-build-release/search 1
