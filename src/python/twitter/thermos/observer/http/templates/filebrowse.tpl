<%doc>
 Template arguments:
   data
   task_id
   filename
   filelen
   read (bytes read)
   offset
   bytes
   has_more
</%doc>

<%def name="download_link()">
  <a href='/download/${task_id}/${filename}'><font size=1>download</font></a>
</%def>

<%def name="less_link()">
  <a href='/file/${task_id}/${filename}?offset=${offset-bytes}&bytes=${bytes}'>&#171; prev</a>
</%def>

<%def name="greater_link()">
  <a href='/file/${task_id}/${filename}?offset=${offset+bytes}&bytes=${bytes}'>next &#187;</a>
</%def>

<html>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>
<style type="text/css">
div.tight
{
  height:85%;
  overflow:auto;
}
</style>

<title>file browser ${task_id}</title>
<body>
 <div class="container">
  <div class="span12">
    <strong> path </strong> ${filename}
  </div>

  <div class="span4">
    ${download_link()}
  </div>
  <div class="span4">
    <strong> size </strong> ${filelen}
  </div>
  <div class="span4">
    <strong> bytes </strong> ${offset}...${offset+read}
  </div>
  <div class="span4">
    % if offset > 0:
      ${less_link()}
    % else:
      &#171; prev
    % endif
    % if has_more:
      ${greater_link()}
    % else:
      next &#187;
    % endif
  </div>

  <div class="span12 tight">
<pre>
${data}
</pre>
  </div>
</div>

</body>
</html>
