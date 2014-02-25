#include "port.h"

#include <stdio.h>

port::port(void)
{
#ifdef __WIN32__
	stdi = _fileno(stdin);
	stdo = _fileno(stdout);
    _setmode(stdi, _O_BINARY);
    _setmode(stdo, _O_BINARY);
#else
	stdi = 0;
	stdo = 1;
#endif
    if (INIT_LOCK(port_r_lock))
        return;
    if (INIT_LOCK(port_w_lock))
        return;
}

bool port::lockr()
{
	return LOCK(port_r_lock);
}

void port::unlockr()
{
	UNLOCK(port_r_lock);
}

bool port::lockw()
{
	return LOCK(port_w_lock);
}

void port::unlockw()
{
	UNLOCK(port_w_lock);
}

int port::read_exact(vector<unsigned char> & buf, unsigned long len)
{
	int i;
	unsigned long got=0;

	if (buf.size() < len)
		buf.resize(len);
	do {
		if ((i = read(stdi, &buf[0]+got, len-got)) <= 0)
			return(i);
		got += i;
	} while (got<len);
	return(len);
}

int port::write_exact(vector<unsigned char> & buf)
{
	int i, wrote = 0;

	int len = (int)buf.size();
	do {
		if ((i = write(stdo, &buf[0]+wrote, len-wrote)) <= 0)
			return (i);
		wrote += i;
	} while (wrote<len);

	return (len);
}

int port::read_cmd(vector<unsigned char> & buf)
{
	int len = 0;
	buf.clear();
	if (lockr()) {
		if(read_exact(buf, 4) < 0) {
			unlockr();
			return (-1);
		}
		len = read_exact(buf, ntohl(*((ul4*)&buf[0])));
		unlockr();
	}
	return len;
}

int port::write_cmd(vector<unsigned char> & buf)
{
	vector<unsigned char> li(sizeof(ul4));
	
	ul4 len = htonl((ul4)buf.size());
	memcpy(&li[0], &len, sizeof(ul4));

	if(lockw()) {
		write_exact(li);
		len = write_exact(buf);
		unlockw();
	}
	return len;
}
