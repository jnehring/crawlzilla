import unittest
from robochecks import RobotsChecker

html_noindex = '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  
  <!-- Prevents search engines from indexing this page -->
  <meta name="robots" content="noindex">

  <title>My Hidden Blog Post</title>
  
  <!-- Optional: Styles -->
  <style>
    body {
      font-family: Arial, sans-serif;
      max-width: 700px;
      margin: 40px auto;
      line-height: 1.6;
      padding: 0 20px;
    }
    h1 {
      color: #333;
    }
    .meta {
      font-size: 0.9em;
      color: #666;
      margin-bottom: 20px;
    }
    article {
      background: #fdfdfd;
      padding: 20px;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
  </style>
</head>
<body>
  <article>
    <h1>My Hidden Blog Post</h1>
    <div class="meta">Written by John Doe on September 15, 2025</div>
    <p>
      This is an example blog article that uses the <code>&lt;meta name="robots" content="noindex"&gt;</code>
      tag in the head section. That means search engines like Google or Bing are instructed 
      <strong>not to index this page</strong>, so it won’t appear in search results.
    </p>
    <p>
      You might want to use this setting for draft posts, private content, or experimental pages 
      that you don’t want visible in search engines.
    </p>
    <p>
      Even though it’s hidden from indexing, anyone with the link can still access it directly.
    </p>
  </article>
</body>
</html>
'''


html_can_index = '''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  
  <title>My Hidden Blog Post</title>
  
  <!-- Optional: Styles -->
  <style>
    body {
      font-family: Arial, sans-serif;
      max-width: 700px;
      margin: 40px auto;
      line-height: 1.6;
      padding: 0 20px;
    }
    h1 {
      color: #333;
    }
    .meta {
      font-size: 0.9em;
      color: #666;
      margin-bottom: 20px;
    }
    article {
      background: #fdfdfd;
      padding: 20px;
      border-radius: 8px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
  </style>
</head>
<body>
  <article>
    <h1>My Hidden Blog Post</h1>
    <div class="meta">Written by John Doe on September 15, 2025</div>
    <p>
      This is an example blog article that uses the <code>&lt;meta name="robots" content="noindex"&gt;</code>
      tag in the head section. That means search engines like Google or Bing are instructed 
      <strong>not to index this page</strong>, so it won’t appear in search results.
    </p>
    <p>
      You might want to use this setting for draft posts, private content, or experimental pages 
      that you don’t want visible in search engines.
    </p>
    <p>
      Even though it’s hidden from indexing, anyone with the link can still access it directly.
    </p>
  </article>
</body>
</html>
'''

class TestHTML2Text(unittest.TestCase):

    def test_tagesschau(self):
        robochecker = RobotsChecker()
        result = robochecker.parse_meta_robots(html_noindex)
        self.assertFalse(result["can_index"])

        result = robochecker.parse_meta_robots(html_can_index)
        self.assertTrue(result["can_index"])

if __name__ == '__main__':
    unittest.main()
