#include <jni.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "SplinterDBJNI.h"
#include "splinterdb/data.h"
#include "splinterdb/default_data_config.h"
#include "splinterdb/public_platform.h"
#include "splinterdb/splinterdb.h"
#include "splinterdb/public_util.h"

#define Mega (1024UL * 1024UL)

splinterdb *splinter;
splinterdb_config cfg; 
data_config default_data_config;


JNIEXPORT jint JNICALL 
Java_SplinterDBJNI_createOrOpen
  (JNIEnv *env, jobject obj, jstring filename, jlong cache_size, jlong disk_size, jint max_key_size, jint max_value_size, jint open_existing) {


	const char *dbname = (*env)->GetStringUTFChars(env, filename, NULL);
	

	default_data_config_init(max_key_size, &default_data_config);


	cfg  =
         	(splinterdb_config) {
                .filename = dbname,
                .cache_size = cache_size * Mega,
                .disk_size = disk_size * Mega,
                .data_cfg = &default_data_config,
        };
	
	int rc;

	if (open_existing) {
		rc = splinterdb_open(&cfg, &splinter);
	} else {
		rc = splinterdb_create(&cfg, &splinter);
	}

	assert(splinter != NULL);

	return rc;
}

JNIEXPORT jint JNICALL Java_SplinterDBJNI_insert
  (JNIEnv *env, jobject obj, jint id, jbyteArray key, jbyteArray value) {
 

	jboolean isCopy;

	char * key_array = (char*)((*env)->GetByteArrayElements(env, key, &isCopy));
	slice slice_key = slice_create((*env)->GetArrayLength( env, key ), key_array);


	char * value_array = (char*)((*env)->GetByteArrayElements(env, value, &isCopy));
        slice slice_value = slice_create((*env)->GetArrayLength( env, value ), value_array);

	int rc = splinterdb_insert(splinter, slice_key, slice_value);

	return rc;

}

JNIEXPORT jbyteArray JNICALL Java_SplinterDBJNI_lookup
  (JNIEnv *env, jobject obj, jint id, jbyteArray key) {

	jboolean isCopy;

	char * key_array = (char*)((*env)->GetByteArrayElements(env, key, &isCopy));
	slice slice_key = slice_create((*env)->GetArrayLength( env, key ), key_array);


	splinterdb_lookup_result result;

	splinterdb_lookup_result_init(splinter, &result, 0, NULL);

	splinterdb_lookup(splinter, slice_key, &result);

	slice value;

	splinterdb_lookup_result_value(splinter, &result, &value);

	char *value_chr = (char *)slice_data(value);


	int length =  sizeof(slice_data(value));


	jbyteArray value_array = (*env)->NewByteArray(env, length);
	(*env)->SetByteArrayRegion(env, value_array, 0, length, (char *)slice_data(value));

	splinterdb_lookup_result_deinit(&result);

	return value_array;
}

JNIEXPORT jint JNICALL Java_SplinterDBJNI_delete
  (JNIEnv *env, jobject obj, jint id, jbyteArray key) {

	jboolean isCopy;

	char * key_array = (char*)((*env)->GetByteArrayElements(env, key, &isCopy));
	slice slice_key = slice_create((*env)->GetArrayLength( env, key ), key_array);


	int rc = splinterdb_delete(splinter, slice_key);

	return rc;

}


JNIEXPORT jint JNICALL Java_SplinterDBJNI_close
  (JNIEnv *env, jobject obj, jint id) {

	splinterdb_close(&splinter);

	return 0;

}

JNIEXPORT jstring JNICALL Java_SplinterDBJNI_version
  (JNIEnv *env, jobject object)
{	
	const char *versionChar = splinterdb_get_version();
	
	jstring version;
   version = (*env)->NewStringUTF(env,versionChar);

	return version;
}


int main (void **args)
{
	return 0;
}
