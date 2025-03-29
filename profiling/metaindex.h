#include <glib.h>
#include <pthread.h>
#include <stdint.h>

// struct defining the values of the Hashtable
typedef struct filemeta {
  uint64_t size;
  int num_access;   // number of times the file has been accessed
  int num_metadata; // number of times the metadata has been accessed
  int num_lseek;    // number of times the file has been lseeked
  int num_read;     // number of times the file has been read
  int num_write;    // number of times the file has been written
  int deleted;      // flag to indicate if the file has been deleted
} Indexmeta;

// structure containing the hashtable structure, global mutex and condition
// variable
typedef struct index {
  GHashTable *htable;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
} Index;

// Initializes the index structure
// Returns NULL in case of failure and a pointer to the struct otherwise
Index *index_init();

// Adds a new key-value entry into the Hashtable
// Returns -1 in case of failure (or if the key already exists) and 0 otherwise
int index_add(Index *index, const char *key, Indexmeta *meta);

// Get the value (meta) for a specific key (key)
// Returns -1 in case of failure (or if the key does not exists) and 0 otherwise
int index_get(Index *index, const char *key, Indexmeta **meta);

// Remove a key-value entry from the Hashtable
// Returns -1 in case of failure (or if the key does not exists) and 0 otherwise
int index_remove(Index *index, char *key);

// Destroys the index structure
void index_destroy(Index *index);

void index_flush(Index *index, char *path);
