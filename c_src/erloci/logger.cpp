/* Copyright 2014 K2Informatics GmbH, Root Längenbold, Switzerland
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "marshal.h"
#include "erl_interface.h"

#include "logger.h"

#define MAX_FORMATTED_STR_LEN 1024
void log_remote(const char * filename, const char * funcname, unsigned int linenumber, unsigned int level, void *term, const char *fmt, ...)
{
    char log_str[MAX_FORMATTED_STR_LEN];
    va_list arguments;
    va_start(arguments, fmt);

#ifdef __WIN32__
    vsprintf_s
#else
    vsnprintf
#endif
    (log_str, MAX_FORMATTED_STR_LEN, fmt, arguments);
    va_end(arguments);

	logger::log(filename, funcname, linenumber, level, term, log_str);
}

logger logger::self;

char * logger::init(int port)
{
#ifdef __WIN32__
    //Start up Winsock
    WSADATA wsadata;

    int error = WSAStartup(0x0202, &wsadata);

    //Did something happen?
    if (error)
        return "unknown error";

    //Did we get the right Winsock version?
    if (wsadata.wVersion != 0x0202)
    {
        WSACleanup(); //Clean up Winsock
        return "bad Winsock version";
    }

    //Fill out the information needed to initialize a socket…
    SOCKADDR_IN target; //Socket address information

    target.sin_family = AF_INET; // address family Internet
    target.sin_port = htons (port); //Port to connect on
    target.sin_addr.s_addr = inet_addr ("127.0.0.1"); //Target IP

    self.log_sock = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP); //Create socket
    if (self.log_sock == INVALID_SOCKET)
    {
        return "Winsock sock create failed"; //Couldn't create the socket
    }  

    //Try connecting...
    if (connect(self.log_sock, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
    {
        return "Winsock sock connect failed"; //Couldn't connect
    }
#else
    struct sockaddr_in serv_addr;
	if((self.log_sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return (char*)"sock create failed";

	memset(&serv_addr, 0, sizeof(serv_addr)); 

	serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port); 

    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)
        return (char*)"inet_pton error occured";

	if(connect(self.log_sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	    return (char*)"sock connect failed";
#endif

	if (INIT_LOCK(self.log_lock)) {
        return "Log write Mutex creation failed\n";
    }

	return NULL; //Success
}

logger::~logger(void)
{
#ifdef __WIN32__
    //Close the socket if it exists
    if (log_sock)
        closesocket(log_sock);

    WSACleanup(); //Clean up Winsock
#else
	close(log_sock);
#endif
}

void logger::log(const char * filename, const char * funcname, unsigned int linenumber, unsigned int level, void *term, const char * log_str)
{
    int tx_len;
    int pkt_len = -1;
    pkt_hdr *hdr;
    unsigned char * tx_buf;

	// Borrowed from write_resp
	ETERM * log = (!term
					? erl_format((char*)"{~i,~s,~s,~i,~s}", level, filename, funcname, linenumber, log_str)
					: erl_format((char*)"{~i,~s,~s,~i,~s,~w}", level, filename, funcname, linenumber, log_str, term));
	tx_len = erl_term_len(log);	
    pkt_len = tx_len+PKT_LEN_BYTES;
    tx_buf = new unsigned char[pkt_len];
    hdr = (pkt_hdr *)tx_buf;
    hdr->len = htonl(tx_len);
    erl_encode(log, tx_buf+PKT_LEN_BYTES);
	erl_free_compound(log);

    if(self.lock()) {
//		send(self.log_sock, log_str, (int)strlen(log_str), 0);
		send(self.log_sock, (char*) tx_buf, pkt_len, 0);
		self.unlock();
    }
	delete tx_buf;

#ifdef __WIN32__
	Sleep(2);
#else
	usleep(2000);
#endif
}