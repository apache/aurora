/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.build

import org.gradle.api.GradleException
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.compile.JavaCompile

class ThriftEntitiesPlugin implements Plugin<Project>  {
  @Override
  void apply(Project project) {

    project.configure(project) {
      apply plugin: ThriftPlugin

      extensions.create('thriftEntities', ThriftEntitiesPluginExtension, project)

      configurations.create('thriftEntitiesCompile')
      configurations.thriftEntitiesCompile.extendsFrom(configurations.thriftRuntime)

      afterEvaluate {
        dependencies {
          thriftEntitiesCompile "com.google.code.gson:gson:${thriftEntities.gsonRev}"
          thriftEntitiesCompile "com.google.guava:guava:${thriftEntities.guavaRev}"
        }
      }
      task('generateThriftEntitiesJava') {
        inputs.files {thriftEntities.inputFiles}
        inputs.files {thriftEntities.codeGenerator}
        outputs.dir {thriftEntities.genJavaDir}
        doLast {
          thriftEntities.genJavaDir.exists() || thriftEntities.genJavaDir.mkdirs()
          thriftEntities.inputFiles.each { File file ->
            exec {
              commandLine thriftEntities.python,
                  thriftEntities.codeGenerator,
                  file.path,
                  thriftEntities.genJavaDir,
                  thriftEntities.genResourcesDir
            }
          }
        }
      }

      task('classesThriftEntities', type: JavaCompile) {
        source files(generateThriftEntitiesJava)
        classpath = configurations.thriftRuntime + configurations.thriftEntitiesCompile
        destinationDir = file(thriftEntities.genClassesDir)
        options.warnings = false
      }

      configurations.create('thriftEntitiesRuntime')
      configurations.thriftEntitiesRuntime.extendsFrom(configurations.thriftEntitiesCompile)
      dependencies {
        thriftEntitiesRuntime files(classesThriftEntities)
      }
      configurations.compile.extendsFrom(configurations.thriftEntitiesRuntime)
      sourceSets.main {
        output.dir(classesThriftEntities)
        output.dir(thriftEntities.genResourcesDir, builtBy: 'generateThriftEntitiesJava')
      }
    }
  }
}

class ThriftEntitiesPluginExtension {
  def python = 'python2.7'
  File genClassesDir
  File genResourcesDir
  File genJavaDir
  FileTree inputFiles
  def codeGenerator

  String gsonRev
  def getGsonRev() {
    if (gsonRev == null) {
      throw new GradleException('thriftEntities.gsonRev is required.')
    } else {
      return gsonRev
    }
  }

  String guavaRev
  def getGuavaRev() {
    if (guavaRev == null) {
      throw new GradleException('thriftEntities.guavaRev is required.')
    } else {
      return guavaRev
    }
  }

  ThriftEntitiesPluginExtension(Project project) {
    genClassesDir = project.file("${project.buildDir}/thriftEntities/classes")
    genResourcesDir = project.file("${project.buildDir}/thriftEntities/gen-resources")
    genJavaDir = project.file("${project.buildDir}/thriftEntities/gen-java")
    inputFiles = project.fileTree("src/main/thrift").matching {
      include "**/*.thrift"
    }
    codeGenerator = "${project.rootDir}/src/main/python/apache/aurora/tools/java/thrift_wrapper_codegen.py"
  }
}
