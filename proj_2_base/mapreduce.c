#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/time.h>
#include "mapreduce.h"
#include "common.h"
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>


void mapreduce(MAPREDUCE_SPEC * spec, MAPREDUCE_RESULT * result)
{
    if (spec == NULL || result == NULL || spec->split_num <= 0 || spec->input_data_filepath == NULL) {
        EXIT_ERROR(ERROR, "Invalid specifications\n");
    }

    int input_fd = open(spec->input_data_filepath, O_RDONLY);
    if (input_fd < 0) {
        EXIT_ERROR(ERROR, "Failed to open input file\n");
    }

    int file_size = lseek(input_fd, 0, SEEK_END);
    lseek(input_fd, 0, SEEK_SET);

    if (file_size <= 0) {
        close(input_fd);
        EXIT_ERROR(ERROR, "Invalid or empty input file\n");
    }

    int split_num = spec->split_num;

    result->map_worker_pid = malloc(split_num * sizeof(int));
    if (!result->map_worker_pid) {
        close(input_fd);
        EXIT_ERROR(ERROR, "Failed to allocate memory for worker PIDs\n");
    }

    char intermediate_files[split_num][256];
    for (int i = 0; i < split_num; i++) {
        snprintf(intermediate_files[i], sizeof(intermediate_files[i]), "mr-%d.itm", i);
    }
    
    struct timeval start, end;

    if (NULL == spec || NULL == result)
    {
        EXIT_ERROR(ERROR, "NULL pointer!\n");
    }
    
    gettimeofday(&start, NULL);

    // printf("file size %d\n", file_size);

    int split_size = file_size / split_num;
    // printf("split_size %d\n", split_size);
    int current_offset = 0;
    int current_offset_array[split_num];
    int adjusted_size_array[split_num];

    for (int i=0; i<split_num; i++){
        
        // printf("split_num %d\n", i);
        // printf("current_offset %d\n", current_offset);
        int adjusted_size = split_size;

        if (i < split_num - 1) {
            lseek(input_fd, current_offset + split_size, SEEK_SET);

            char c;
            while (read(input_fd, &c, 1) > 0) {
                adjusted_size++;      
                // if (c == '.' || c == ',' || c == '!') {  
                if(c == '\n' | c == '.'){
                    break;
                }
            }
        } else {
            adjusted_size = file_size - current_offset;
            if (adjusted_size < 0 ) {
                adjusted_size = 0;
            }
        }

        current_offset_array[i] = current_offset;
        adjusted_size_array[i] = adjusted_size;

        // printf("adjusted size %d\n", adjusted_size);
        // printf("adjusted size + current offset %d\n", current_offset+adjusted_size);
        current_offset += adjusted_size;

    }

    // for (int i = 0; i < split_num; i++){
    //     printf("%d %d %d\n", current_offset_array[i], adjusted_size_array[i], current_offset_array[i]+adjusted_size_array[i] );
    // }

    for (int i = 0; i < split_num; i++){

        pid_t pid = fork();
        if (pid < 0) {
            close(input_fd);
            EXIT_ERROR(ERROR, "Failed to fork map worker process\n");
        }

        if (pid == 0) {  
            int fd_out = open(intermediate_files[i], O_CREAT | O_WRONLY | O_TRUNC, 0666);

            if (fd_out < 0) {
                EXIT_ERROR(ERROR, "Failed to create intermediate file\n");
            }

            int worker_fd = dup(input_fd); 
            lseek(worker_fd, current_offset_array[i], SEEK_SET);

            DATA_SPLIT split = {
                .fd = worker_fd,
                .size = adjusted_size_array[i],
                .usr_data = spec->usr_data
            };

            if (spec->map_func(&split, fd_out) < 0) {
                EXIT_ERROR(ERROR, "Map function failed\n");
            }

            close(fd_out);
            close(worker_fd);
            exit(0);

        }
        else{

            result->map_worker_pid[i] = pid;
            int status;
            waitpid(result->map_worker_pid[i], &status, 0);
            if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
                close(input_fd);
                EXIT_ERROR(ERROR, "Map worker process failed\n");

            }
        }
    }

    int intermediate_fds[split_num];
    for (int i = 0; i < split_num; i++) {
        intermediate_fds[i] = open(intermediate_files[i], O_RDONLY);
        if (intermediate_fds[i] < 0) {
            EXIT_ERROR(ERROR, "Failed to open intermediate file\n");
        }
    }

    char result_file[] = "result.txt";
    int result_fd = open(result_file, O_CREAT | O_WRONLY | O_TRUNC, 0666);
    if (result_fd < 0) {
        EXIT_ERROR(ERROR, "Failed to create result file\n");
    }

    pid_t reduce_pid = fork();
    if (reduce_pid < 0) {
        EXIT_ERROR(ERROR, "Failed to fork reduce worker process\n");
    }

    if (reduce_pid == 0) {  
        if (spec->reduce_func(intermediate_fds, split_num, result_fd) < 0) {
            EXIT_ERROR(ERROR, "Reduce function failed\n");
        }

        exit(0);  
    } else {
        result->reduce_worker_pid = reduce_pid;
    }

    for (int i = 0; i < split_num; i++) {
        close(intermediate_fds[i]);
    }

    int status;
    waitpid(result->reduce_worker_pid, &status, 0);
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        EXIT_ERROR(ERROR, "Reduce worker process failed\n");
    }

    close(input_fd);
    close(result_fd);

    result->filepath = strdup(result_file);

    gettimeofday(&end, NULL);   

    result->processing_time = (end.tv_sec - start.tv_sec) * US_PER_SEC + (end.tv_usec - start.tv_usec);
}
