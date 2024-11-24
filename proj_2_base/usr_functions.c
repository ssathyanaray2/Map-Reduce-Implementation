#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include "common.h"
#include "usr_functions.h"


/* User-defined map function for the "Letter counter" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int letter_counter_map(DATA_SPLIT * split, int fd_out)
{
    if (!split || split->fd < 0 || fd_out < 0) {
        return -1;
    }
    char *buffer = (char *)malloc(split->size);
    if (!buffer) {
        perror("Memory allocation failed");
        return -1;
    }

    ssize_t bytes_read = read(split->fd, buffer, split->size);
    if (bytes_read < 0) {
        perror("Error reading from file descriptor");
        free(buffer);
        return -1;
    }

    int letter_counts[26] = {0};

    for (ssize_t i = 0; i < bytes_read; i++) {
        if (isalpha(buffer[i])) {
            char letter = toupper(buffer[i]);
            letter_counts[letter - 'A']++;
        }
    }

    for (int i = 0; i < 26; i++) {
        char output_line[32];
        int len = snprintf(output_line, sizeof(output_line), "%c %d\n", 'A' + i, letter_counts[i]);
        if (write(fd_out, output_line, len) != len) {
            perror("Error writing to intermediate file");
            free(buffer);
            return -1;
        }
    }

    free(buffer);
    return 0;
    
}

/* User-defined reduce function for the "Letter counter" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int letter_counter_reduce(int * p_fd_in, int fd_in_num, int fd_out) {

    if (p_fd_in == NULL || fd_in_num <= 0 || fd_out < 0) {
        return -1; 
    }

    int letter_counts[26] = {0}; 

    for (int i = 0; i < fd_in_num; i++) {
        FILE *file = fdopen(p_fd_in[i], "r");
        if (file == NULL) {
            return -1; 
        }

        char line[128]; 
        while (fgets(line, sizeof(line), file)) {
            char letter;
            int count;

            if (sscanf(line, "%c %d", &letter, &count) == 2) {
                if (letter >= 'A' && letter <= 'Z') {
                    letter_counts[letter - 'A'] += count; 
                }
            }
        }

        fclose(file);  
    }

    for (int i = 0; i < 26; i++) {
        char output[32];
        int len = snprintf(output, sizeof(output), "%c %d\n", 'A' + i, letter_counts[i]);
        write(fd_out, output, len); 
    }

    return 0; 
}

/* User-defined map function for the "Word finder" task.  
   This map function is called in a map worker process.
   @param split: The data split that the map function is going to work on.
                 Note that the file offset of the file descripter split->fd should be set to the properly
                 position when this map function is called.
   @param fd_out: The file descriptor of the itermediate data file output by the map function.
   @ret: 0 on success, -1 on error.
 */
int word_finder_map(DATA_SPLIT * split, int fd_out)
{   
    if (!split || split->fd < 0 || !split->usr_data) {
        fprintf(stderr, "Invalid data split\n");
        return -1;
    }

    const char *target_word = (const char *)split->usr_data;  
    size_t target_len = strlen(target_word);

    if (target_len == 0) {
        fprintf(stderr, "Target word is empty\n");
        return -1;
    }

    char *buffer = malloc(split->size + 1);
    if (!buffer) {
        perror("Failed to allocate memory for the buffer");
        return -1;
    }

    ssize_t bytes_read = read(split->fd, buffer, split->size);
    if (bytes_read < 0) {
        perror("Failed to read from input file");
        free(buffer);
        return -1;
    }

    buffer[bytes_read] = '\0';  

    char *line = NULL;
    char *start = buffer;      
    char *end = buffer;        

    while (end < buffer + bytes_read) {

        if (*end == '\n' || end == buffer + bytes_read - 1) {
            if (*end == '\n') {
                *end = '\0';  
            } else {
                end++;  
            }

            line = start;

            char *match = strstr(line, target_word);
            int found = 0;

            while (match) {
                if ((match == line || !isalnum(*(match - 1))) &&       
                    (!isalnum(*(match + target_len)))) {              
                    found = 1;
                    break;
                }
                match = strstr(match + target_len, target_word);      
            }

            if (found) {
                if (dprintf(fd_out, "%s\n", line) < 0) {
                    perror("Failed to write to intermediate file");
                    free(buffer);
                    return -1;
                }
            }

            start = end + 1;
        }
        end++;
    }

    free(buffer);
    return 0;
}

/* User-defined reduce function for the "Word finder" task.  
   This reduce function is called in a reduce worker process.
   @param p_fd_in: The address of the buffer holding the intermediate data files' file descriptors.
                   The imtermeidate data files are output by the map worker processes, and they
                   are the input for the reduce worker process.
   @param fd_in_num: The number of the intermediate files.
   @param fd_out: The file descriptor of the final result file.
   @ret: 0 on success, -1 on error.
   @example: if fd_in_num == 3, then there are 3 intermediate files, whose file descriptor is 
             identified by p_fd_in[0], p_fd_in[1], and p_fd_in[2] respectively.

*/
int word_finder_reduce(int * p_fd_in, int fd_in_num, int fd_out)
{
    if (!p_fd_in || fd_in_num <= 0 || fd_out < 0) {
        fprintf(stderr, "Invalid arguments passed to reduce function.\n");
        return -1;
    }

    char *line = NULL;
    size_t line_capacity = 0;
    ssize_t line_len;

    for (int i = 0; i < fd_in_num; i++) {
        FILE *file_in = fdopen(p_fd_in[i], "r");
        if (!file_in) {
            perror("Failed to open intermediate file");
            return -1;
        }

        while ((line_len = getline(&line, &line_capacity, file_in)) != -1) {
            if (write(fd_out, line, line_len) == -1) {
                perror("Failed to write to result file");
                fclose(file_in);
                free(line);
                return -1;
            }
        }

        fclose(file_in);
    }

    free(line);

    return 0;
}


