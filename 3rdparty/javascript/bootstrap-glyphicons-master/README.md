Bootstrap Glyphicons Support
============================

[Twitter's Bootstrap v2](http://twitter.github.com/bootstrap) project already uses GLYPHICONS halflings (created by [Jan Kovařík](http://glyphicons.com/)) and are released for Bootstrap under the Apache 2.0 License. What this project aims to accomplish is add seamless support for the 400+ GLYPHICONS (available for free under the [Creative Commons Attribution 3.0 Unported (CC BY 3.0)](http://creativecommons.org/licenses/by/3.0/deed.en) license) to Bootstrap so "large" icons can be used. To achieve this I've combined the over 400 24x24 GLYPHICONS in to a Sprite and added icon-large definitions.

Whenever possible larger GLYPHICONS halflings names have been mapped. Otherwise the CSS class definition follows the names set by the files in the zip.

To use this within your site you **NEED** to do the following:

 1. Download `bootstrap.icon-large.min.css` and place it in the same directory as bootstrap.css file
 2. Download `glyphicons.png` and place it in the same directory as glyphicons-halflings.png
 3. Add the following CSS definition under the bootstrap.css call
     `<link href="css/bootstrap.icon-large.min.css" rel="stylesheet">`
 4. Clearly visible on the site (like the footer) add a link to [glyphicons.com](http://www.glyphicons.com/). This is a [requirement by the artist](http://glyphicons.com/glyphicons-licenses/) unless you purchase the GLYPHICONS ALL or GLYPHICONS PRO plans. If you don't want to give attribution to the artist, at least pay him for his fantastic work.

That's it. You can find an entire listing of all the GLYPHICONS
