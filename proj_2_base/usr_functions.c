#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include "common.h"
#include "usr_functions.h"

#define BUFFER_SIZE 4096

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
        return -1; // Error handling
    }

    int letter_counts[26] = {0}; // For letters A-Z

    // Read from each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        FILE *file = fdopen(p_fd_in[i], "r");
        if (file == NULL) {
            return -1; // Error handling
        }

        char line[128]; // Buffer for reading each line
        while (fgets(line, sizeof(line), file)) {
            char letter;
            int count;

            // Parse line in the format "X 123"
            if (sscanf(line, "%c %d", &letter, &count) == 2) {
                if (letter >= 'A' && letter <= 'Z') {
                    letter_counts[letter - 'A'] += count; // Aggregate counts
                }
            }
        }

        fclose(file); // Close the current intermediate file
    }

    // Write the aggregated results to the final output file
    for (int i = 0; i < 26; i++) {
        char output[32];
        int len = snprintf(output, sizeof(output), "%c %d\n", 'A' + i, letter_counts[i]);
        write(fd_out, output, len); // Write to the output file
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
    if (!split || split->fd < 0 || split->usr_data == NULL) {
        return -1; 
    }

    char *target_word = (char *)split->usr_data; // Target word provided by the user
    int target_len = strlen(target_word);

    if (target_len == 0) {
        return -1; 
    }

    char *buffer = malloc(split->size + 1); 
    if (!buffer) {
        return -1; 
    }

    if (read(split->fd, buffer, split->size) != split->size) {
        free(buffer);
        printf("reading error\n");
        return -1; 
    }

    buffer[split->size] = '\0'; 

    char *line_start = buffer; 
    char *line_end;

    while ((line_end = strchr(line_start, '.')) != NULL) {
        *line_end = '\0'; 

        char *match = strstr(line_start, target_word);
        int found = 0;

        while (match) {
            // Check for whole word match
            if ((match == line_start || isspace(*(match - 1))) && // Start of the line or preceded by a space
                (isspace(*(match + target_len)) || *(match + target_len) == '\0')) { // End of the line or followed by a space
                found = 1;
                break;
            }
            match = strstr(match + 1, target_word); // Search for the next occurrence
        }

        if (found) {
            // Write the matching line to the intermediate file
            dprintf(fd_out, "%s\n", line_start);
        }

        line_start = line_end + 1; // Move to the start of the next line
    }

    // Handle any remaining data in the buffer after the last newline
    if (*line_start != '\0') {
        char *match = strstr(line_start, target_word);
        int found = 0;

        while (match) {
            if ((match == line_start || isspace(*(match - 1))) &&
                (isspace(*(match + target_len)) || *(match + target_len) == '\0')) {
                found = 1;
                break;
            }
            match = strstr(match + 1, target_word);
        }

        if (found) {
            dprintf(fd_out, "%s\n", line_start);
        }
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
    char buffer[1024];  // Buffer to read intermediate files
    int bytes_read;
    char *line;  // Pointer to each line of content
    size_t line_len = 0;

    // Use a set or map to avoid duplicate lines in the output
    // For simplicity, we use a static array here.
    // In a real-world scenario, a hash table or set should be used to efficiently detect duplicates.
    char *seen_lines[1024];  // Array to store seen lines
    int seen_count = 0;  // To keep track of number of unique lines seen

    // Process each intermediate file
    for (int i = 0; i < fd_in_num; i++) {
        lseek(p_fd_in[i], 0, SEEK_SET);  // Set each file descriptor to the beginning

        // Read the intermediate file in chunks
        while ((bytes_read = read(p_fd_in[i], buffer, sizeof(buffer) - 1)) > 0) {
            buffer[bytes_read] = '\0';  // Null-terminate the buffer

            // Split the buffer into lines
            line = strtok(buffer, "\n");
            while (line != NULL) {
                // Check if this line has been seen already
                int is_duplicate = 0;
                for (int j = 0; j < seen_count; j++) {
                    if (strcmp(seen_lines[j], line) == 0) {
                        is_duplicate = 1;  // Line already exists in the result
                        break;
                    }
                }

                // If not a duplicate, write the line to the output file
                if (!is_duplicate) {
                    write(fd_out, line, strlen(line));  // Write the line to result file
                    write(fd_out, "\n", 1);  // Add newline after the line
                    seen_lines[seen_count++] = line;  // Mark this line as seen
                }

                // Move to the next line
                line = strtok(NULL, "\n");
            }
        }
    }

    return 0;

}


