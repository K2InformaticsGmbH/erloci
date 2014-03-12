all:
	MSBuild c_src/erlocisln.sln /t:Build /p:Configuration=Debug /p:Platform="x64" /m

clean:
	MSBuild c_src/erlocisln.sln /t:Clean /p:Configuration=Debug /p:Platform="x64" /m
