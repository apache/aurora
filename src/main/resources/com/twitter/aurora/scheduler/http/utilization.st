<html>
  <head>
    <title>Aurora $cluster_name$ slaves</title>
    <link href="/css/bootstrap.min.css" rel="stylesheet" />
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
    $if(exception)$
      <font color='red'>Exception: $exception$</font>
    $endif$
  </body>
</html>
