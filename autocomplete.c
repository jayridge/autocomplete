#include <stdio.h>
#include <simplehttp/simplehttp.h>
#include "uthash.h"
#include "utlist.h"

#define NAME        "autocomplete"
#define VERSION     "0.1"
#define DEBUG       1


struct el_ctx {
    char *key;
    char *data;
    int ref_count;
    UT_hash_handle hh;
};

typedef struct el {
    char *key;
    time_t when;
    struct el_ctx *elx;
    struct el *next, *prev;
} el;

struct el_result {
    struct el *e;
    int nelems;
    UT_hash_handle hh;
};

struct namespace {
    char *name;
    int nelems;
    el *head;
    struct el_ctx *ctx_table;
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

struct el_ctx *get_el_ctx(struct namespace *ns, char *key)
{
    struct el_ctx *elx = NULL;
    if (ns && key) {
        HASH_FIND_STR(ns->ctx_table, key, elx);
    }
    return elx;
}

/*
 *  First sort by key, then reverse chrono.
 */
int keycmp(el *a, el *b) {
    int rc = strcmp(a->key, b->key);
    if (rc == 0) {
        if (a->when > b->when) {
            return -1;
        } else if (a->when < b->when) {
            return 1;
        }
    }
    return rc;
}

int prefix_time_cmp(el *a, el *b) {
    fprintf(stderr, "prefix_time_cmp %s %s\n", a->key, b->key);
    int rc = strncmp(a->key, b->key, strlen(b->key));
    if (0 && rc == 0) {
        if (a->when > b->when) {
            return -1;
        } else if (a->when < b->when) {
            return 1;
        }        
    }
    return rc;
}

/*
 * Reverse chrono for trimming.
 */
int timecmp(el *a, el *b) {
    if (a->when > b->when) {
        return -1;
    } else if (a->when < b->when) {
        return 1;
    }
    return 0;
}

/*
 * Reverse chrono then key.
 */
int time_key_cmp(el *a, el *b) {
    if (a->when > b->when) {
        return -1;
    } else if (a->when < b->when) {
        return 1;
    }
    return strcmp(a->key, b->key);
}

void print_namespace(struct namespace *ns)
{
    struct el *e;
    DL_FOREACH(ns->head, e) {
        fprintf(stderr, "%s %ld %s\n", e->key, e->when, (e->elx->data ? e->elx->data : "none"));
    }
}

void put_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *namespace, *key, *data;
    struct namespace *ns;
    struct el *e;
    struct el_ctx *elx;
    time_t when = time(NULL);
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key = (char *)evhttp_find_header(&args, "key");
    data = (char *)evhttp_find_header(&args, "data");
    
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
         *  struct el_ctx
         */
        elx = get_el_ctx(ns, key);
        if (!elx) {
            elx = malloc(sizeof(*elx));
            memset(elx, 0, sizeof(*elx));
            elx->key = strdup(key);
        }
        if (data) {
            if (elx->data) {
                free(elx->data);
            }
            elx->data = strdup(data);
        }
        elx->ref_count += 1;
        HASH_ADD_KEYPTR(hh, ns->ctx_table, elx->key, strlen(elx->key), elx);

        /*
         *  struct el
         */
        e = malloc(sizeof(*e));
        memset(e, 0, sizeof(*e));
        e->key = strdup(key);
        e->when = when;
        e->elx = elx;
        DL_APPEND(ns->head, e);
        DL_SORT(ns->head, keycmp);
        
        /*
         *  TODO: resort and trim if reached max_elems.
         */
                
        print_namespace(ns);
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
    } else {
        evbuffer_add_printf(evb, "missing argument: key\n");
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_ARG_KEY", evb);
    }
    
    evhttp_clear_headers(&args);
}

void search_cb(struct evhttp_request *req, struct evbuffer *evb, void *ctx)
{
    struct evkeyvalq args;
    char *namespace, *key;
    struct namespace *ns;
    struct el *e, *eptr, etmp;
    struct el_ctx *elx;
    struct el_result *er, *results = NULL;
    time_t when = time(NULL);
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    key = (char *)evhttp_find_header(&args, "key");
    
    if (DEBUG) {
        fprintf(stderr, "/search_cb %s %s\n", namespace, key);
    }
    
    if (namespace && key) {
        ns = get_namespace(namespace);
        if (ns) {
            memset(&etmp, 0, sizeof(etmp));
            etmp.key = key;
            DL_SEARCH(ns->head, e, &etmp, prefix_time_cmp);
            while (e) {
                fprintf(stderr, "found %s %ld %p %p\n", e->key, e->when, e->next, e->prev);
                HASH_FIND_STR(results, e->key, er);
                if (!er) {
                    er = malloc(sizeof(*er));
                    er->e = e;
                    HASH_ADD_KEYPTR(hh, results, er->e->key, strlen(er->e->key), er);
                    fprintf(stderr, "new entry: %s\n", e->key);
                }
                er->nelems += 1;
                e = e->next;
                if (!e || strncmp(e->key, key, strlen(key)) != 0) {
                    break;
                }
            }
            
            HASH_SRT(hh, results, time_key_cmp);
            for(er=results; er != NULL; er=(struct el_result*)(er->hh.next)) {
                e = er->e;
                fprintf(stderr, "RES: %s %ld %s\n", e->key, e->when, (e->elx->data ? e->elx->data : "none"));
            }

            /*
             *  TODO: free the list.
             */
        }
        evhttp_send_reply(req, HTTP_OK, "OK", evb);
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
