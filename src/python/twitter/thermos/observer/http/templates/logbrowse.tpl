<%doc>
 Template arguments:
   uid
   process
   run
   logtype
   data
   filelen
   read (bytes read)
   offset
   bytes
   has_more
</%doc>

<%def name="less_link()">
  <a href='/logs/${uid}/${process}/${run}/$logtype}?offset=${offset-bytes}&bytes=${bytes}'>&#171; prev</a>
</%def>

<%def name="greater_link()">
  <a href='/logs/${uid}/${process}/${run}/$logtype}?offset=${offset+bytes}&bytes=${bytes}'>next &#187;</a>
</%def>

<html>

<link rel="stylesheet"
      type="text/css"
      href="/assets/bootstrap.css"/>

<title>log browser ${uid}</title>

<body>
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

  <div class="span16">
<pre>
${data}
</pre>
  </div>

</body>
</html>
