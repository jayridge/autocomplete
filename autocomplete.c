#include <stdio.h>
#include <simplehttp/simplehttp.h>
#include "uthash.h"
#include "utlist.h"
#include "json/json.h"

#define NAME        "autocomplete"
#define VERSION     "0.1"
#define DEBUG       1


typedef struct el {
    char *key;
    char *data;
    time_t when;
    int count;
    UT_hash_handle hh;  /* handle for key hash */
    UT_hash_handle rh;  /* handle for results hash */
} el;

struct namespace {
    char *name;
    int nelems;
    struct el *elems;
    UT_hash_handle hh;
};

struct namespace *spaces = NULL;
int max_elems = 10;

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);
void search_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx);



struct namespace *get_namespace(char *namespace)
{
    struct namespace *ns = NULL;
    HASH_FIND_STR(spaces, namespace, ns);
    return ns;
}

int key_sort(el *a, el *b) {
    return strcmp(a->key, b->key);
}

int time_count_sort(el *a, el *b) {
    if (a->when > b->when) {
        return -1;
    } else if (a->when < b->when) {
        return 1;
    } else {
        if (a->count > b->count) {
            return -1;
        } else if (a->count < b->count) {
            return 1;
        }
    }
    return 0;
}

void print_namespace(struct namespace *ns)
{
    struct el *e;
    for(e=ns->elems; e != NULL; e=e->hh.next) {
        fprintf(stderr, "%s %ld %s\n", e->key, e->when, (e->data ? e->data : "none"));
    }
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *namespace, *key, *data, *ts;
    struct namespace *ns;
    struct el *e;
    time_t when = time(NULL);
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key = (char *)evhttp_find_header(&args, "key");
    data = (char *)evhttp_find_header(&args, "data");
    ts = (char *)evhttp_find_header(&args, "ts");
    if (ts) {
        when = (time_t)strtol(ts, NULL, 10);
    }
    
    if (DEBUG) {
        fprintf(stderr, "/put_cb %s %s\n", namespace, key);
    }
    
    if (namespace && key) {
        /*
         *  struct namespace
         */
        ns = get_namespace(namespace);
        if (!ns) {
            ns = malloc(sizeof(*ns));
            memset(ns, 0, sizeof(*ns));
            ns->name = strdup(namespace);
            HASH_ADD_KEYPTR(hh, spaces, ns->name, strlen(ns->name), ns);
        }
        
        /*
         *  struct el
         */
        HASH_FIND_STR(ns->elems, key, e);
        if (!e) {
            e = malloc(sizeof(*e));
            memset(e, 0, sizeof(*e));
            e->key = strdup(key);
            HASH_ADD_KEYPTR(hh, ns->elems, e->key, strlen(e->key), e);
            HASH_SORT(ns->elems, key_sort);
        }
        if (e->data) {
            free(e->data);
        }
        e->data = (data ? strdup(data) : NULL);
        e->when = when;
        e->count += 1;
        
        /*
         *  TODO: resort and trim if reached max_elems.
         */
                
        print_namespace(ns);
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    } else {
        evbuffer_add_printf(evb, "missing argument: [ key | namespace ]\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    }
    
    evhttp_clear_headers(&args);
}

void search_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *namespace, *key;
    struct json_object *jsobj, *jsel, *jsresults;
    struct el *e, *results = NULL;
    struct namespace *ns;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key = (char *)evhttp_find_header(&args, "key");
    
    if (DEBUG) {
        fprintf(stderr, "/search_cb %s %s\n", namespace, key);
    }
    
    if (namespace && key) {
        jsobj = json_object_new_object();
        json_object_object_add(jsobj, "namespace", json_object_new_string(namespace));
        json_object_object_add(jsobj, "key", json_object_new_string(key));
        jsresults = json_object_new_array();
        ns = get_namespace(namespace);
        if (ns) {
#define key_match(x) (strncmp(((struct el*)x)->key,key,strlen(key)) == 0)
            HASH_SELECT(rh, results, hh, ns->elems, key_match);
            HASH_SRT(rh, results, time_count_sort);
            for (e=results; e != NULL; e=e->rh.next) {
                jsel = json_object_new_object();
                json_object_object_add(jsel, "key", json_object_new_string(e->key));
                json_object_object_add(jsel, "when", json_object_new_int(e->when));
                json_object_object_add(jsel, "count", json_object_new_int(e->count));
                json_object_object_add(jsel, "data", json_object_new_string(e->data));
                json_object_array_add(jsresults, jsel);
                fprintf(stderr, "elem key %s\n", e->key);
            }
            HASH_CLEAR(rh, results);
        }
        json_object_object_add(jsobj, "results", jsresults);
        evbuffer_add_printf(evb, "%s\n", (char *)json_object_to_json_string(jsobj));
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
        json_object_put(jsobj);
    } else {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    }
    
    evhttp_clear_headers(&args);
}

void info()
{
    fprintf(stdout, "%s: autocomplete server.\n", NAME);
    fprintf(stdout, "Version: %s, https://github.com/jayridge/autocomplete\n", VERSION);
}

int version_cb(int value)
{
    fprintf(stdout, "Version: %s\n", VERSION);
    return 0;
}

int main(int argc, char **argv)
{
    define_simplehttp_options();    
    if (!option_parse_command_line(argc, argv)) {
        return 1;
    }
    
    info();    
    simplehttp_init();
    simplehttp_set_cb("/put?*", put_cb, NULL);
    simplehttp_set_cb("/search?*", search_cb, NULL);
    simplehttp_main();
    free_options();
    
    return 0;
}
