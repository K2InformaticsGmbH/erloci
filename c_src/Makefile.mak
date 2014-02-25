all:
	MSBuild c_src/erlocisln.sln /t:Rebuild /p:Configuration=Debug

clean:
	MSBuild c_src/erlocisln.sln /t:Clean /p:Configuration=Debug
