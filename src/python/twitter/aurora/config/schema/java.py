from textwrap import dedent

from twitter.thermos.config.schema import *


class Java(Struct):
  arguments                 = Required(String)
  collector                 = Required(String)
  heap                      = Required(String)
  new_gen                   = Required(String)
  perm_gen                  = Required(String)
  code_cache                = Required(String)
  cpu_cores                 = Required(String)
  gc_log                    = Default(String, 'gc.log')
  extra_jvm_flags           = Default(String, '')
  java_home                 = Required(String)
  best_practice_jvm_flags   = Required(String)
  print_gc_jvm_flags        = Required(String)
  oome_jvm_flags            = Required(String)
  agentlib                  = Default(String, '')
  canary_package_version    = Default(String, 'live')

Java7 = Java(
  java_home                 = '/usr/lib/jvm/java-1.7.0-openjdk',
  best_practice_jvm_flags   = '-XX:+CMSScavengeBeforeRemark',
  print_gc_jvm_flags        = '-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC -XX:+PrintTenuredClassDistribution',
  oome_jvm_flags            = '-XX:+HeapDumpOnOutOfMemoryError',
  agentlib                  = '-agentlib:perfagent'
)

Canary = Java7(
  java_home                 = 'canary'
)

Java6 = Java(
  java_home                 = '/usr/lib/jvm/java-1.6.0',
  best_practice_jvm_flags   = '-XX:+CMSScavengeBeforeRemark -XX:+CMSClassUnloadingEnabled',
  print_gc_jvm_flags        = '-verbosegc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintHeapAtGC',
  oome_jvm_flags            = '-XX:+HeapDumpOnOutOfMemoryError',
)

DEFAULT_PERM_GEN = 128
DEFAULT_CODE_CACHE = 64
CMDLINE = ''' 
    export JAVA_HOME={{__profile.java_home}} &&
    export PATH={{__profile.java_home}}/bin:$PATH &&
    export _JAVA_OPTIONS=" -Xmx{{__profile.heap}}M -Xms{{__profile.heap}}M -Xmn{{__profile.new_gen}}M {{__profile.perm_gen}}  {{__profile.code_cache}} -XX:ParallelGCThreads={{__profile.cpu_cores}} -Xloggc:{{__profile.gc_log}} {{__profile.best_practice_jvm_flags}} {{__profile.print_gc_jvm_flags}} {{__profile.oome_jvm_flags}} -Dmesos.resources.cpu={{__profile.cpu_cores}} {{__profile.collector}} {{__profile.agentlib}} {{__profile.extra_jvm_flags}} " &&
    {{__profile.java_home}}/bin/java {{__profile.arguments}}'''
DOWNLOAD_CANARY_PACKAGE = '''  
    _RH_VERSION=`grep -Eo [0-9] /etc/redhat-release |head -1` &&
    if [ "$_RH_VERSION" -eq "5" ]; then
      echo "CentOS $_RH_VERSION, downloading jdk package version {{__profile.canary_package_version}}";
      {{packer[jvm-release][centos5jdk][{{__profile.canary_package_version}}].copy_command}};
      tar -xvf centos5jdk.tar.gz;
    elif [ "$_RH_VERSION" -eq "6" ]; then
      echo "CentOS $_RH_VERSION, downloading jdk package version {{__profile.canary_package_version}}";
      {{packer[jvm-release][centos6jdk][{{__profile.canary_package_version}}].copy_command}};
      tar -xvf centos6jdk.tar.gz;
    else
      echo "Appropriate JDK Package could not be downloaded. OS version not supported or OS version mis-match";
    fi'''

def JVMProcess(arguments, resources, name='Java', jvm=Java7, **kw):
  """Returns a JVM Process and exports correct values for JAVA_HOME, PATH and _JAVA_OPTIONS.
     :param arguments: Specifies the arguments that will be used to run the java Process
      Example: If you want to run java -version, then "-version"  will be arguments
     :type arguments: string
     :param resources: Specifies the resources that will be used for this Job.
      This will be used to set the jvm config and also provide the JVM with the available cpu cores
     :type resources: Resources
     :param jvm: Specifies the JVM class that will have JVM config.
     :type jvm: JVM
     Usage:

     RESOURCES = Resources(cpu = 1.0, ram = 1024*MB, disk = 128*MB)
 
     JOB_TEMPLATE = Job(
      name = 'java_setup_sh',
      role = os.environ['USER'],
      cluster = 'smfd',
      task = SequentialTask(
        name = 'task',
        resources = RESOURCES,
        processes = [
          JVMProcess(" -version", resources=RESOURCES, name='birdname', jvm=Java7)
        ]
      )
     )
    
     Wiki Page: http://confluence.twitter.biz/display/RUNTIMESYSTEMS/Mesos+JVM+Process+Template

  """
  # Template Process
  template_process = Process(
    name = name,
    **kw)

  # Validate name
  assert name is not '', "Process Name cannot be empty"

  # Get Resources
  cpu = resources.cpu().get()
  ram = resources.ram().get()

  # Add Arguments to JVM
  jvm = jvm(arguments = arguments)

  # Available RAM in MB
  normalized_ram = (ram / 1024) / 1024

  # Check if the user gave us values for heap, if not, use the available RAM in resources
  if jvm.heap() is not Empty:
    heap_ram = (int(jvm.heap().get()) / 1024) / 1024
    # Since the User has supplied a heap size, no effort will be made to size down the heap.  
    adaptive_heap_size = False
  else:
    heap_ram = normalized_ram
    adaptive_heap_size = True

  # Set Code Cache
  code_cache, code_cache_string = get_code_cache(jvm, heap_ram)

  # Set Perm Gen
  perm_gen, perm_gen_string = get_perm_gen(jvm, heap_ram)

  # Subtract perm_gen and code_cache from heap_ram
  if adaptive_heap_size:
    heap_ram = heap_ram - perm_gen - code_cache
  
  # Set Young Gen
  new_gen = get_young_gen(jvm, heap_ram)

  # Set CPU Cores
  cpu_cores = get_cpu_cores(jvm, cpu)

  # Set Collector
  collector = get_collector(jvm)

  # Check Canary Version use 
  check_canary_version(jvm)

  # Get JAVA_HOME
  java_home, is_canary = get_java_home(jvm)

  # Build the rest of the JVM Configuration
  jvm = jvm(
    java_home    = java_home,
    collector    = collector,
    heap         = heap_ram,
    new_gen      = new_gen,
    perm_gen     = perm_gen_string,
    code_cache   = code_cache_string,
    cpu_cores    = cpu_cores
  )

  # Do Sanity Checks
  sanity_checks(jvm)

  # Warn the user if they are trying to use more memory than what has been allocated.
  if heap_ram + perm_gen + code_cache > normalized_ram:
    print('JVM Config: Overcommitted Process - Memory usage is more than memory allocated.')

  if is_canary is False:
    return template_process(
      cmdline = dedent(CMDLINE)
    ).bind(__profile = jvm)
  else:
    return template_process(
      cmdline = dedent(DOWNLOAD_CANARY_PACKAGE + CMDLINE)
    ).bind(__profile = jvm)

def get_code_cache(jvm, heap_ram):
  """Returns the Code Cache Size and jvm flag string
  String is empty if allocated RAM is < 512MB
  Code Cache is set to 64MB if allocated RAM is >= 512MB
  User set value will always override any automatic sizing
  """
  # Check if the user gave us Code Cache size
  if jvm.code_cache() is not Empty:
    code_cache = (int(jvm.code_cache().get()) / 1024) / 1024
    code_cache_string = '-XX:ReservedCodeCacheSize=%sM' % code_cache
  else:
    if heap_ram < 512:
      code_cache = 0
      code_cache_string = ''
    else:
      code_cache = DEFAULT_CODE_CACHE
      code_cache_string = '-XX:ReservedCodeCacheSize=%sM' % DEFAULT_CODE_CACHE
  return code_cache, code_cache_string

def get_perm_gen(jvm, heap_ram):
  """Returns the Permanent Generation Size and jvm flag string
  String is Empty if allocated RAM is < 512MB
  Permanent Generation is set to 128MB if allocated RAM is >= 512MB
  User set value will always override any automatic sizing
  """
  if jvm.perm_gen() is not Empty:
    perm_gen = (int(jvm.perm_gen().get()) / 1024) / 1024
    perm_gen_string = '-XX:MaxPermSize=%sM -XX:PermSize=%sM ' % (perm_gen, perm_gen)
  else:
    if heap_ram < 512:
      perm_gen = 0
      perm_gen_string = ''
    else:
      perm_gen = DEFAULT_PERM_GEN
      perm_gen_string = '-XX:MaxPermSize=%sM -XX:PermSize=%sM ' % (DEFAULT_PERM_GEN, DEFAULT_PERM_GEN)
  return perm_gen, perm_gen_string

def get_young_gen(jvm, heap_ram):
  """Returns the Young Gen Size.
  Young Gen is 1/2 of Heap by Default
  """
  # Check if the user gave us Young Gen
  if jvm.new_gen() is not Empty:
    new_gen = (int(jvm.new_gen().get()) / 1024) / 1024
  else:
    new_gen = heap_ram / 2
  return new_gen

def get_cpu_cores(jvm, cpu):
  """Returns the CPU cores to be used by the VM
  AssertionError if this value is bigger than allocated CPU Cores in resource definition.
  """
  if jvm.cpu_cores() is not Empty:
    assert int(jvm.cpu_cores().get()) <= cpu, "cpu_cores cannot be more than allocated CPUs. cpu_cores: %s ; Allocated: %s" % (jvm.cpu_cores().get(), cpu)
    cpu_cores = int(jvm.cpu_cores().get())
  else:
    cpu_cores = int(cpu)
  return cpu_cores

def get_collector(jvm):
  """Returns the Collector Flag.
  AssertionError if user wants G1 on JDK6
  """
  if jvm.collector() is not Empty:
    if jvm.collector().get().lower() == 'latency':
      collector = '-XX:+UseConcMarkSweepGC'
    elif jvm.collector().get().lower() == 'throughput':
      collector = '-XX:+UseParallelOldGC'
    elif jvm.collector().get().lower() == 'g1':
      assert 'java-1.6' not in jvm.java_home().get(), "G1 is experimental in JDK6uX; Use latency or throughput"
      collector = '-XX:+UseG1GC'
    else:
      collector = '-XX:+UseConcMarkSweepGC'
  else:
    collector = '-XX:+UseConcMarkSweepGC'
  return collector

def sanity_checks(jvm):
  """Checks that JAVA_HOME, java arguments and configuration is valid.
  AssertionError on failure
  """
  assert jvm.java_home().get() is not Empty, "JAVA_HOME not supplied"
  assert jvm.arguments().get() is not Empty, "Java Arguments not supplied"
  assert jvm.check().ok(), "JVM Configuration Error: %s" % jvm

def check_canary_version(jvm):
  """Checks the JDK Canary Version to be used
  AssertionError if the canary_package_version is not defined when using Canary JDK.
  """
  if jvm.java_home().get() == 'canary':
    assert jvm.canary_package_version().get() is not '', "canary_package_version should be live or a valid version number" 

def get_java_home(jvm):
  """Returns the correct JAVA_HOME path for canaries
  AssertionError if JAVA_HOME is Empty"""
  assert jvm.java_home().get() is not Empty, "JAVA_HOME not supplied"
  is_canary = False
  if jvm.java_home().get().lower() == 'canary':
    java_home = './java-1.7.0-openjdk7'
    is_canary = True
  else:
    java_home = jvm.java_home().get()
  return java_home, is_canary
