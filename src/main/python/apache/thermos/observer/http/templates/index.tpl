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
<title>thermos({{hostname}})</title>

<link rel="stylesheet"
      type="text/css"
      href="assets/bootstrap.css"/>

<script src="assets/jquery.js"></script>
<link rel="icon" href="/assets/favicon.ico">
</head>

<body>

<div class="container" id="defaultLayout">
  <div id="activeTaskContainer" class='uber-container'>
    <div class="active-container" data-url="main/active">
    </div>
  </div>
  <br><br>
  <div id="finishedTaskContainer" class='uber-container'>
    <div class="finished-container" data-url="main/finished">
    </div>
  </div>
</div>

<script type="text/javascript">

$(document).on('click', 'a.refresh-container', function(e) {
   e.preventDefault()
   topLevelDivContainer = $(this).closest('.uber-container')
   divDataUrl = $(this).attr('data-url')
   $.ajax({
      'type': 'GET',
      'dataType': 'html',
      'url': divDataUrl,
      success: function(data, xhr, err) {
        $(topLevelDivContainer).html(data)
      }
   })
 })

refreshDivs = function() {
  $('#activeTaskContainer').load($('.uber-container .active-container').attr('data-url'))
  $('#finishedTaskContainer').load($('.uber-container .finished-container').attr('data-url'))
}

$(document).bind('ready', refreshDivs)
setInterval(refreshDivs, 10000)

</script>

</body>
</html>
