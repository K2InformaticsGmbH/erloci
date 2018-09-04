#include <sys/time.h>
#include <stdio.h>
#include <unistd.h>

int main(void)
{
    struct timeval start, end;
    long mtime, secs, usecs;

    gettimeofday(&start, NULL);
    sleep(1);
    gettimeofday(&end, NULL);

    secs  = end.tv_sec  - start.tv_sec;
    usecs = end.tv_usec - start.tv_usec;
    mtime = ((secs) * 1000 + usecs/1000.0) + 0.5;
    printf("Elapsed time: %ld millisecs\n", mtime);

    return 0;
}
