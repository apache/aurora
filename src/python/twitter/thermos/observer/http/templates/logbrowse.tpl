<%def name="download_link()">
  <a href='/download/${task_id}/${filename}'><font size=1>download</font></a>
</%def>

<html>

<head>
  <meta charset="utf-8">
  <title></title>

  <style type="text/css">
    .log {
      font-family: "Inconsolata", "Monaco", "Courier New", "Courier";
      line-height:14px;
      font-size: 12px;
    }

    .invert {
      color: #FFFFFF;
      text-decoration: none;
      background: #000000;
    }
  </style>
</head>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>
<style type="text/css">
div.tight
{
  height:100%;
  overflow:scroll;
}
</style>

<title>log browser ${task_id}</title>
<body>
  <div> <strong> log </strong> ${logtype} </div>
  <div style="position: absolute; left: 5px; top: 0px;">
    <p id="indicator" class="log invert"></p>
  </div>

  <div id="data" class="log" style="white-space:pre-wrap; background-color:#EEEEEE;"></div>
</body>

<script src="/assets/jquery.js"></script>
<script src="/assets/jquery.pailer.js"></script>

<script>
  function resize() {
    var margin_left = parseInt($('body').css('margin-left'));
    var margin_top = parseInt($('body').css('margin-top'));
    var margin_bottom = parseInt($('body').css('margin-bottom'));
    $('#data').width($(window).width() - margin_left);
    $('#data').height($(window).height() - margin_top - margin_bottom);
  }

  $(window).resize(resize);

  $(document).ready(function() {
    resize();

    $('#data').pailer({
      'read': function(options) {
        var settings = $.extend({
          'offset': -1,
          'length': -1
        }, options);

        var url = "/logdata/${task_id}/${process}/${run}/${logtype}"
          + '?offset=' + settings.offset
          + '&length=' + settings.length;
        return $.getJSON(url);
      },
      'indicator': $('#indicator')
    });
  });
</script>
</html>
