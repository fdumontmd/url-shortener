index(url, shorter, done, error) ::= <<
<html>
	<head>
		<title>URL Shortener Service</title
	</head>
	<body>
		<h1>URL Shortener Service Admininstration</h1>
		$if (error)$
		<p>The requested keyword $shorter$ is already in use. Please select another one</p>
		$endif$
		
		$if (done)$
		<p>The requested keyword $shorter$ is mapped to URL $url$</p>
		$endif$
		
		<form action="" method="post">
			<label for="url">URL: <input type="text" id="url" name="url" value="$if(!done)$$url$$endif$"/> </label> <br/>
			<label for="shorter">Keyword: <input type="text" id="shorter" name="shorter" value=""/> </label> <br/>
			<input type="submit" name="Submit"/>
		</form>
	</body>
</html>
>>