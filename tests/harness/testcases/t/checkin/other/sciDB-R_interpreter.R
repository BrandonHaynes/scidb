library('scidb',warn=FALSE)
scidbconnect()
iquery("build(<v:bool>[i=1:1,1,0],true)",return=TRUE)

