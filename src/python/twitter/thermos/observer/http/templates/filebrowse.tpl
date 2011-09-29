<%doc>
 Template arguments:
   data
   uid
   filename
   filelen
   read (bytes read)
   offset
   bytes
   has_more
</%doc>

<%def name="download_link()">
  <a href='/download/${uid}/${filename}'><font size=1>dl</font></a>
</%def>

<%def name="less_link()">
  <a href='/file/${uid}/${filename}?offset=${offset-bytes}&bytes=${bytes}'>&#171;</a>
</%def>

<%def name="greater_link()">
  <a href='/file/${uid}/${filename}?offset=${offset+bytes}&bytes=${bytes}'>&#187;</a>
</%def>

<html>

<title>Log Printer</title>
<body>
  <h3>task ${uid}<h3><br>
  <h3>path ${filename}, size ${filelen}</h3><br>
  <h3>bytes ${offset}...${offset+read}</h3>
  % if offset > 0:
    ${less_link()}
  % else:
    &#171;
  % endif
  % if has_more:
    ${greater_link()}
  % else:
    &#187;
  % endif
  <br>
  <br>

<pre>
${data}
</pre>
</body>

</html>
