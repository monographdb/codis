.DEFAULT_GOAL = all

SOURCE := jemalloc-4.4.0

all:
	@test -f $(SOURCE)/Makefile || make config --quiet

config:
	cd $(SOURCE) && ./autogen.sh --with-jemalloc-prefix="je_"
	@make -f help.mk --quiet relink

clean distclean:
	@test -f $(SOURCE)/Makefile && make -C $(SOURCE) --quiet distclean || true
	@make -f help.mk --quiet rmlink

install: all
	go install ./
