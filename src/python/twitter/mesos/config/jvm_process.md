# DO NOT EDIT THIS PAGE DIRECTLY
### This file is managed in git - edit the markdown source at `src/python/twitter/mesos/jvm_process.md`, and publish with `./pants goal confluence src/python/twitter/mesos:mesos-jvmprocess`


## Overview
This page outlines the features of the new JVM Process Template that can used to set up a Java Process in your mesos client configuration.

**Why Should I Use this?**

It configures the JVM for you so you don't have to deal with cryptic JVM flags. If you have a tuned set of flags then you can use them as well in a more intuitive way.

##Template

Here is an example mesos client configuration that will be referred to throughout this document. For experienced mesos users, the only new thing should be "JVMProcess".

JVMProcess is a helper to configure our JVM. It requires a Resources definition and arguments for the Java Process.

Want a default JDK7 configuration?
---------------------------------

	import os
        from twitter.mesos.config.schema import *
 
        RESOURCES = Resources(cpu = 1.0, ram = 1024*MB, disk = 128*MB)
 
        JOB_TEMPLATE = Job(
         name = 'java_setup_sh',
         role = os.environ\['USER'\],
         cluster = 'smfd',
         task = SequentialTask(
           name = 'task',
           resources = RESOURCES,
           processes = [
             JVMProcess(arguments = ' -version', resources=RESOURCES)
           ]
          )
        )
 
 
        jobs = [
          JOB_TEMPLATE,
        ]


You can also explicitly specify JDK7 and Process Name(Optional)
-----------------------------------------------------

	import os
	from twitter.mesos.config.schema import *
 
	RESOURCES = Resources(cpu = 1.0, ram = 1024*MB, disk = 128*MB)
 
	JOB_TEMPLATE = Job(
	 name = 'java_setup_sh',
	 role = os.environ\['USER'\],
	 cluster = 'smfd',
	 task = SequentialTask(
	   name = 'task',
	   resources = RESOURCES,
	   processes = [
	     JVMProcess(arguments = ' -version', resources=RESOURCES, name='ProcessName', jvm=Java7() )
	   ]
	  )
	)
 
 
	jobs = [
	  JOB_TEMPLATE,
	]
----------------------------

You can also use JDK6 by replacing "Java7" with "Java6"
----------------------------
	import os
	from twitter.mesos.config.schema import *
 
	RESOURCES = Resources(cpu = 1.0, ram = 1024*MB, disk = 128*MB)
 
	JOB_TEMPLATE = Job(
	  name = 'java_setup_sh',
	  role = os.environ['USER'],
	  cluster = 'smfd',
	  task = SequentialTask(
	    name = 'task',
	    resources = RESOURCES,
	    processes = [
	    JVMProcess(arguments = ' -version', resources=RESOURCES, name='ProcessName', jvm=Java6() )
	    ]
	  )
	)
 
	jobs = [
	 JOB_TEMPLATE,
	]
----------------------------


##Default JVM Configuration

If you don't want to deal with configuring the JVM, you can start with a default JVM configuration (as does the template above). With a Default template it will look at the available resource and configure the JVM accordingly.

JDK7 default JVM process

	JVMProcess(arguments = ' -version', resources=RESOURCES, jvm=Java7() )

JDK6 Default JVM Process

	JVMProcess(arguments = ' -version', resources=RESOURCES, jvm=Java6() )

Named JVM Process

	JVMProcess(arguments = ' -version', resources=RESOURCES, name='ProcessName', jvm=Java[6|7]() )

Default set of Flags (JDK7)

	-Xmx832M -Xms832M -Xmn416M -XX:MaxPermSize=128M -XX:PermSize=128M -XX:ReservedCodeCacheSize=64M
	-XX:ParallelGCThreads=1 -Xloggc:gc.log -XX:+CMSScavengeBeforeRemark -verbosegc -XX:+PrintGCDetails
	-XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC
	-XX:+PrintTenuredClassDistribution -XX:+HeapDumpOnOutOfMemoryError -Dmesos.resources.cpu=1
	-XX:+UseConcMarkSweepGC -agentlib:perfagent

The Default JVM Configuration is generic so it might not be ideal as far as performance goes. It does however set up a starting point with "best practice" flags.


##Heap Sizing Logic

We will look at the whole list of flags later. For now, lets look at how the heap sizing is done.

The basis for all default configuration is the resources definition and there are two classes of JVM configuration. One where RAM is <512MB and one where RAM >=512MB.

* RAM < 512MB
:  In this case, we will not explicitly set Perm Gen and Code Cache.  
:  Heap will be 512M and New Gen will be 1/2 * 512.  

* RAM >= 512MB
:  In this case, we will explicitly set Perm Gen to 128M and Code Cache to 64M  
:  Heap will be set to: Available RAM - PermGen - Code Cache  
:  New Gen will be set to 1/2 of the derived Heap.  


##Customizing your JVM Configuration

If you have a tuned JVM, you can easily port that configuration.

Here are the JVMProcess attributes and what they do:

* java_home
:  value gets exported to JAVA_HOME and PATH.
:  Also used to call the java process.
:  Default for JDK7: /usr/lib/jvm/java-1.7.0-openjdk
:  Default for JDK6: /usr/lib/jvm/java-1.6.0
:  Setting an invalid path will cause the process to fail.
* collector
:  Valid Values for JDK7: latency, throughput, G1
:  Valid Values for JDK6: latency, throughput
:  Latency sets -XX:+UseConcMarkSweepGC
:  Throughput sets -XX:+UseParallelOldGC
:  G1 sets -XX:+UseG1GC
:  Default is -XX:+UseConcMarkSweepGC for JDK6 and JDK7
* heap
:  Sets -Xmx and -Xms
:  Defaults are explained in "Heap Sizing Logic"
* new_gen
:  Sets -Xmn
:  Defaults are explained in "Heap Sizing Logic"
* perm_gen
:  Sets -XX:MaxPermSize and -XX:PermSize
:  Default Value is 128MB
* code_cache
:  Sets -XX:ReservedCodeCacheSize
:  Default Value is 64MB
* cpu_cores
:  Sets -XX:ParallelGCThreads and -Dmesos.resources.cpu
:  Default is from resources definition.
* gc_log
:  Sets -Xloggc.
:  Please Make sure that the directory path exists if you are overriding this.
:  Default is "gc.log"
* extra_jvm_flags
:  Users can use this to add any flag they want to the config. Examples would be CMS Flags, Survivor Ratio/Tenuring Flags, G1 flags, etc.
:  This can also be used to override any default JVM config as set up by JvmProcess (also: If you are doing this, let us know and we can work with you to incorporate your use case)

##Examples

Lets go through some examples of how to set up your JVM Config.

Here is the Resources definition I will be using for these examples:

	RESOURCES = Resources(cpu = 1.0, ram = 1024*MB, disk = 128*MB)

###Scenario 1: Setting Heap and New Gen  

    JVMProcess(arguments = ' -version', resources=RESOURCES, jvm=Java7(heap = 1024*MB, new_gen = 768*MB) )

Which gives us the following Config  

    -Xmx1024M -Xms1024M -Xmn768M -XX:MaxPermSize=128M -XX:PermSize=128M -XX:ReservedCodeCacheSize=64M -XX:ParallelGCThreads=1 -Xloggc:gc.log
    -XX:+CMSScavengeBeforeRemark -verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
    -XX:+PrintHeapAtGC -XX:+PrintTenuredClassDistribution -XX:+HeapDumpOnOutOfMemoryError -Dmesos.resources.cpu=1 -XX:+UseConcMarkSweepGC -agentlib:perfagent

Notice that Xmx is set to the value that was requested by the user. In This config, we are using more memory than we asked for (i.e. 1024MB vs what we are using 1024 + 128 + 64). We do this because user's decision takes priority over default sizing and configuration. We will also try to warn the user:

    kpaul@kpaul:~/workspace/science$ ./pants py aurora:client  create  java_setup_sh ../testmesos/testing.mesos 
    JVM Config: Overcommitted Process - Memory usage is more than memory allocated.
     INFO] Creating ssh tunnel for smfd
     INFO] Creating job java_setup_sh
     INFO] Response from scheduler: OK (message: 1 new tasks pending for job kpaul/java_setup_sh)
     INFO] Job url: http://smfd-akg-07-sr2.devel.twitter.com:8081/scheduler/kpaul/devel/java_setup_sh

We don't want Mesos to Kill us, so lets modify the configuration so we are only using the amount of memory that we asked for. We can shrink the heap by (128 + 64 ) 192MB or we can shrink Heap and Perm Gen and Code Cache by an appropriate amount.Please keep in mind that setting perm gen below 64M and code cache below 48M could cause performance issues or even OOMe.

    JVMProcess(arguments = ' -version', resources=RESOURCES, jvm=Java7(heap = 908*MB, new_gen = 768*MB, perm_gen = 64*MB, code_cache = 48*MB) )

Which Gives us:

    -Xmx908M -Xms908M -Xmn768M -XX:MaxPermSize=64M  -XX:PermSize=64M  -XX:ReservedCodeCacheSize=48M -XX:ParallelGCThreads=1 -Xloggc:gc.log
    -XX:+CMSScavengeBeforeRemark -verbosegc  -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps  -XX:+PrintTenuringDistribution
    -XX:+PrintHeapAtGC -XX:+PrintTenuredClassDistribution -XX:+HeapDumpOnOutOfMemoryError -Dmesos.resources.cpu=1 -XX:+UseConcMarkSweepGC -agentlib:perfagent

###Scenario 2: Setting more options and pushing Extra flags.

Lets take an example of a running service and try to port that to JVMProcess.

    /usr/lib/jvm/java-1.7.0-openjdk/bin/java -Xmx912m -Xms912m -XX:NewSize=512m -XX:MaxDirectMemorySize=200m -XX:+DisableExplicitGC -XX:+UseParallelOldGC -XX:+UseAdaptiveSizePolicy
    -XX:MaxGCPauseMillis=1000 -XX:GCTimeRatio=99 -verbosegc -Xloggc:gc.log -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
    -XX:+PrintHeapAtGC -DadminPort=31189 -DmesosAdminPort=31744 -Ddatacenter=smf1 -Denv=prod -DshardId=0 -jar tweetbutton.aggregator-2.0.28-SNAPSHOT.jar -f config/smf1.scala

This is how the ported JVMProcess looks like:

    JVMProcess(arguments ='-DadminPort=31189 -DmesosAdminPort=31744 -Ddatacenter=smf1 -Denv=prod -DshardId=0
    -jar tweetbutton.aggregator-2.0.28-SNAPSHOT.jar -f config/smf1.scala', resources=RESOURCES, jvm=Java(heap = 912*MB, new_gen = 512*MB, collector = 'throughput',
    extra_jvm_flags = '-XX:MaxDirectMemorySize=200m -XX:+DisableExplicitGC -XX:+UseAdaptiveSizePolicy -XX:MaxGCPauseMillis=1000 -XX:GCTimeRatio=99' ) )

##JVM Flag Summary

We will Quickly go over the flags that we set by default.

* Heap Size - These Flags set the Max Heap and Initial Heap size
:  -Xmx832M
:  -Xms832M

* Young Gen Size - This Flag Sets the NewSize and MaxNewSize
:  -Xmn416M

* Perm Size - Setting Initial and Max Perm Gen Size
:  -XX:MaxPermSize=128M
:  -XX:PermSize=128M

* Code Cache - Setting the Max Code Cache Size
:  -XX:ReservedCodeCacheSize=64M

* GC Threads - The Number of GC Threads
:  -XX:ParallelGCThreads=1

* Print GC Flags - These are a group of flags that set up GC Logs.
:  -Xloggc:gc.log
:  -verbosegc
:  -XX:+PrintGCDetails
:  -XX:+PrintGCTimeStamps
:  -XX:+PrintGCDateStamps
:  -XX:+PrintTenuringDistribution
:  -XX:+PrintHeapAtGC
:  -XX:+PrintTenuredClassDistribution (not in JDK6)

* OOMe Flags - In the case of an OOMe. Dump the Heap so we can look at live data inside the heap.
:  -XX:+HeapDumpOnOutOfMemoryError

* This exposes the available CPUs for the job
:  -Dmesos.resources.cpu=1

* Collector: latency, throughput, g1 translates into 
:  -XX:+UseConcMarkSweepGC, -XX:+UseParallelGC and -XX:+UseG1GC respectively.
:  -XX:+UseConcMarkSweepGC is default

* Start up the JVM with perf agent for profiling
:  -agentlib:perfagent

* Collect Young Before a Remark.
:  -XX:+CMSScavengeBeforeRemark



