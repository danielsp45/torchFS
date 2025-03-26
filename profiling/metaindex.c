#include "metaindex.h"
#include <stdio.h>

Index* index_init() {

	//allocate memory for the index
	Index *index = malloc(sizeof(Index));

	//GHashTable initialization
	index->htable = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);

	//mutex variable initialization
	//int pthread_mutex_init(pthread_mutex_t *mutex,const pthread_mutexattr_t *attr);
	//attr can be used to define non-default attributes (e.g., recursive lock)
	pthread_mutex_init(&index->mutex, NULL);

	return index;
}

//Note: remember that memory allocation and copying must be done here
int index_add(Index *index, const char* key, Indexmeta* meta) {
    pthread_mutex_lock(&index->mutex);

    if(g_hash_table_contains(index->htable, key)){
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }

    char *nkey = strdup(key);
    g_hash_table_insert(index->htable, nkey, meta);

    pthread_mutex_unlock(&index->mutex);

    return 0;
}

int index_get(Index *index, const char* key, Indexmeta** meta) {
    pthread_mutex_lock(&index->mutex);

    *meta = g_hash_table_lookup(index->htable, key);
    if (*meta == NULL) {
        pthread_mutex_unlock(&index->mutex);
        return -1;
    }

    pthread_mutex_unlock(&index->mutex);
    
    return 0;
}

int index_remove(Index* index, char* key){

	pthread_mutex_lock(&index->mutex);

	Indexmeta *res = g_hash_table_lookup(index->htable, key);
	if(res==NULL){
		pthread_mutex_unlock(&index->mutex);
		return -1;
	}

	g_hash_table_remove(index->htable,key);
	
	pthread_mutex_unlock(&index->mutex);

	return 0;
}

void index_destroy(Index* index){

	//destroy hashtable
	g_hash_table_destroy(index->htable);

	//Useful for exercise 2.2
	//destroy mutex and cond variables
	pthread_mutex_destroy(&index->mutex);
    pthread_cond_destroy(&index->cond);

	free(index);
}

void index_flush(Index* index, char* path) {
    FILE *file = fopen(path, "w");
    if (file == NULL) {
        printf("Error opening file!\n");
        return;  // Handle the error gracefully
    }

    printf("Flushing index to file\n");

    // Write to file
    pthread_mutex_lock(&index->mutex);
    GHashTableIter iter;
    gpointer key, value;
    g_hash_table_iter_init(&iter, index->htable);
    while (g_hash_table_iter_next(&iter, &key, &value)) {
        fprintf(file, "%s SIZE:%ld NUM_ACCESS:%d NUM_META:%d NUM_LSEEK:%d NUM_READ:%d NUM_WRITE:%d DEL:%d\n",
                (char*) key, ((Indexmeta*) value)->size, ((Indexmeta*) value)->num_access,
                ((Indexmeta*) value)->num_metadata, ((Indexmeta*) value)->num_lseek,
                ((Indexmeta*) value)->num_read, ((Indexmeta*) value)->num_write,
                ((Indexmeta*) value)->deleted);

        // Print to the console
        printf("%s SIZE:%ld NUM_ACCESS:%d NUM_META:%d NUM_LSEEK:%d NUM_READ:%d NUM_WRITE:%d DEL:%d\n",
               (char*) key, ((Indexmeta*) value)->size, ((Indexmeta*) value)->num_access,
               ((Indexmeta*) value)->num_metadata, ((Indexmeta*) value)->num_lseek,
               ((Indexmeta*) value)->num_read, ((Indexmeta*) value)->num_write,
               ((Indexmeta*) value)->deleted);
    }
    pthread_mutex_unlock(&index->mutex);

    // Close the file
    fclose(file);
}
