<html>
<title>thermos({{hostname}})</title>

<link rel="stylesheet"
      type="text/css"
      href="assets/bootstrap.css"/>

<script src="assets/mootools-core.js"> </script>
<script src="assets/observer.js"> </script>

<body>

<div id="topbar">task index</div>

<div id="workspace">
  <div class="fixed-container" id="defaultLayout">
    <div id="activeTaskContainer">
    </div>
    <br><br>
    <div id="finishedTaskContainer">
    </div>
  </div>
</div>

<script type="text/javascript">

onDomReady = function() {
  activeTableManager = new TableManager('active')
  finishedTableManager = new TableManager('finished')
  $('activeTaskContainer').adopt(activeTableManager.element)
  $('finishedTaskContainer').adopt(finishedTableManager.element)
}

window.addEvent("domready", onDomReady)

</script>

</body>
</html>
