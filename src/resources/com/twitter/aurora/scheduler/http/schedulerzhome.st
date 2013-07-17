<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Aurora $cluster_name$ scheduler</title>
    <link href="/css/bootstrap.min.css" rel="stylesheet" />
    <link href="/css/jquery.dataTables.css" rel="stylesheet" />
    <script type="text/javascript" src="/js/jquery.min.js"></script>
    <script type="text/javascript" src="/js/bootstrap.min.js"></script>
    <script type="text/javascript" src="/js/jquery.dataTables.min.js"></script>
    <script type="text/javascript" src="/js/dataTables.bootstrap.js"></script>
    <script type="text/javascript" src="/js/dataTables.localstorage.js"></script>
  </head>
  <body>
    <div class="container-fluid">
      <div class="row-fluid">
        <center>
          <h2>Scheduled Jobs</h2>
        </center>
      </div>
      
      <table id="jobs-table" class="table table-bordered table-striped table-condensed table-hover">
        <thead>
          <th>Role
          <th>Jobs
          <th>Cron Jobs
        </thead>
        <tbody>
          $owners:{ owner |
          <tr>
            <td><a href="/scheduler/$owner.role$">$owner.role$</a>
            <td>$owner.jobCount$
            <td>$owner.cronJobCount$
          </tr>
          }$
        </tbody>
      </table>
      $if(exception)$
        <font color='red'>Exception: $exception$</font>
      $endif$
    </div>
    
    <script type="text/javascript">
      jQuery(function(){
        jQuery('#jobs-table').dataTable({
          'bStateSave': true
        });
      });
    </script>
  </body>
</html>
