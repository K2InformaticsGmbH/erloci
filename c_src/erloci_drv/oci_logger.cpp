/* Copyright 2012 K2Informatics GmbH, Root Laengenbold, Switzerland
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
#ifdef __WIN32__
#include <Winsock2.h>
#else
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>
#endif

#ifdef __WIN32__
SOCKET log_socket; //Socket handle
#else
int log_socket;
#endif

//CONNECTTOHOST – Connects to a remote host
char * connect_tcp(int PortNo)
{
#ifdef __WIN32__
    //Start up Winsock…
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
    target.sin_port = htons (PortNo); //Port to connect on
    target.sin_addr.s_addr = inet_addr ("127.0.0.1"); //Target IP

    log_socket = socket (AF_INET, SOCK_STREAM, IPPROTO_TCP); //Create socket
    if (log_socket == INVALID_SOCKET)
    {
        return "Winsock sock create failed"; //Couldn't create the socket
    }  

    //Try connecting...

    if (connect(log_socket, (SOCKADDR *)&target, sizeof(target)) == SOCKET_ERROR)
    {
        return "Winsock sock connect failed"; //Couldn't connect
    }
    else
        return NULL; //Success
#else
    struct sockaddr_in serv_addr;
	if((log_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0)
        return (char*)"sock create failed";

	memset(&serv_addr, 0, sizeof(serv_addr)); 

	serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PortNo); 

    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr)<=0)
        return (char*)"inet_pton error occured";

	if(connect(log_socket, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
	    return (char*)"sock connect failed";

    return NULL; //Success
#endif
}

//CLOSECONNECTION – shuts down the socket and closes any connection on it
void close_tcp()
{
#ifdef __WIN32__
    //Close the socket if it exists
    if (log_socket)
        closesocket(log_socket);

    WSACleanup(); //Clean up Winsock
#else
	close(log_socket);
#endif
}

#include "oci_marshal.h"
#include "erl_interface.h"

#define MAX_FORMATTED_STR_LEN 1024
#ifdef __WIN32__
extern HANDLE log_write_mutex;
#else
extern pthread_mutex_t log_write_mutex;
#endif
void log_remote(const char * filename, const char * funcname, unsigned int linenumber, unsigned int level, const char *fmt, ...)
{
    int tx_len;
    int pkt_len = -1;
    pkt_hdr *hdr;
    unsigned char * tx_buf;

    char log_str[MAX_FORMATTED_STR_LEN];
    va_list arguments;
    va_start(arguments, fmt);

#ifdef __WIN32__
    vsprintf_s
#else
    vsnprintf
#endif
    (log_str, MAX_FORMATTED_STR_LEN, fmt, arguments);

	// Borrowed from write_resp
	ETERM * log = erl_format((char*)"{~i,~s,~s,~i,~s}", level, filename, funcname, linenumber, log_str);
	tx_len = erl_term_len(log);	
    pkt_len = tx_len+PKT_LEN_BYTES;
    tx_buf = new unsigned char[pkt_len];
    hdr = (pkt_hdr *)tx_buf;
    hdr->len = htonl(tx_len);
    erl_encode(log, tx_buf+PKT_LEN_BYTES);

    if(lock(log_write_mutex)) {
//		send(log_socket, log_str, (int)strlen(log_str), 0);
		send(log_socket, (char*) tx_buf, pkt_len, 0);
		unlock(log_write_mutex);
    }

	delete tx_buf;

    va_end(arguments);

#ifdef __WIN32__
	Sleep(2);
#else
	usleep(2000);
#endif
}
