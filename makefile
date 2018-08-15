

all : IndexBenchCompile  

IndexBenchCompile :
	-cd src ;$(MAKE)
	
clean:
	-cd src; $(MAKE) clean
