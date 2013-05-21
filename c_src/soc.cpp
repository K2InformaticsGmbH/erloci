#include<sys/socket.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<netdb.h>
#include<arpa/inet.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>

int main(int argc, char *argv[])
{
    struct sockaddr_in serv_addr;
    int s;
    if((s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("socket create failed!\n");
        return -1;
    }

    memset(&serv_addr, 0, sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(atoi(argv[1]));

    if(inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
        printf("inet_pton error!\n");
        return -1;
    }

    if(connect(s, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
        printf("socket connect failed\n");
        return -1;
    }

    char payload[100];
    do {
        printf("> ");
        scanf("%s", payload);
        send(s, payload, strlen(payload), 0);
    } while(true);
 
    return 0;
}
