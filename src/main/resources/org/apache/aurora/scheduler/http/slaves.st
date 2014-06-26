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
    <title>Aurora $cluster_name$ slaves</title>
    <link href="/css/bootstrap.min.css" rel="stylesheet" />
  </head>
  <body>
    <h2>Slave Hosts</h2>
    <table class="table table-striped table-condensed table-hover">
      <thead>
        <th>Host
        <th>Slave ID
        <th>Maintenance mode
        <th>Attributes
      </thead>
      <tbody>
        $slaves:{ slave |
        <tr>
          <td><a href="http://$slave.host$:8081">$slave.host$</a>
          <td>$slave.id$
          <td>$slave.mode$
          <td>$slave.attributes$
        </tr>
        }$
      </tbody>
    </table>
    $if(exception)$
      <font color='red'>Exception: $exception$</font>
    $endif$
  </body>
</html>
