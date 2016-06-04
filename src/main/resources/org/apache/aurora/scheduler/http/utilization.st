<!--
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->
 <html>
  <head>
    <title>Aurora $cluster_name$ utilization</title>
    <link href="/assets/bower_components/bootstrap/dist/css/bootstrap.min.css" rel="stylesheet" />
  </head>
  <body>
    <h2>Cluster utilization</h2>
    <table class="table table-striped table-condensed table-hover">
      <thead>
        <th>Type
        <th>CPU
        <th>RAM
        <th>Disk
      </thead>
      <tbody>
        $metrics:{ metric |
        <tr>
          <td>
          $if(metric.link)$
            <a href="/utilization/$metric.link$">$metric.title$</a>
          $else$
            $metric.title$
          $endif$
          <td>$metric.cpu$
          <td>$metric.ramGb$ GB
          <td>$metric.diskGb$ GB
        </tr>
        }$
      </tbody>
    </table>
  </body>
</html>
