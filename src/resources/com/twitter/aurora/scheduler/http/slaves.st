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
