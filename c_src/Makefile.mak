all:
	if not exist "priv" mkdir priv
	MSBuild c_src/erlocisln.sln /t:Build /p:Configuration=$(conf) /p:Platform="x64" /m
	@echo Build $(conf)

clean:
	MSBuild c_src/erlocisln.sln /t:Clean /p:Configuration=$(conf) /p:Platform="x64" /m
	del priv\*.lib priv\*.dll priv\*.exe /f /q
	@echo Clean $(conf)
