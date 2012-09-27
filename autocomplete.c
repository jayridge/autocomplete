#include <event.h>
#include <evhttp.h>
#include <stdio.h>
#include <unicode/utypes.h>
#include <unicode/ustring.h>
#include <unistd.h>

#include "json/json.h"
#include "uthash.h"

#define NAME "autocomplete"
#define VERSION "0.1"
#define DEBUG 1

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

struct evhttp *httpd;
struct namespace *spaces = NULL;
int max_elems = 10;

void put_cb(struct evhttp_request *req, void *arg);
void search_cb(struct evhttp_request *req, void *arg);
void incr_cb(struct evhttp_request *req, void *arg);
void decr_cb(struct evhttp_request *req, void *arg);
void delete_cb(struct evhttp_request *req, void *arg);

struct namespace *get_namespace(char *namespace)
{
    struct namespace *ns = NULL;
    HASH_FIND_STR(spaces, namespace, ns);
    return ns;
}

int key_sort(el *a, el *b) {
    return strcasecmp(a->key, b->key);
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

void free_el(struct el *e)
{
    if (e) {
        if (e->key) {
            free(e->key);
        }
        free(e);
    }
}

void print_namespace(struct namespace *ns)
{
    struct el *e;
    for(e=ns->elems; e != NULL; e=e->hh.next) {
        fprintf(stderr, "%s %ld %s\n", e->key, e->when, (e->data ? e->data : "none"));
    }
}

/*
 * tolower a utf8 str using the correct icu conversion with opt locale.
 */
char *utf8_tolower(char *s, char *locale)
{
    UChar *buf = NULL;
    char *buf2 = NULL;
    UErrorCode err = U_ZERO_ERROR;
    int32_t len, len2;
    
    // utf8 to uchar
    u_strFromUTF8(NULL, 0, &len, s, -1, &err);
    buf = malloc(sizeof(UChar) * len+1);
    err = U_ZERO_ERROR;
    u_strFromUTF8(buf, len+1, &len, s, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strFromUTF8 failed %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }
    
    // uchar tolower
    err = U_ZERO_ERROR;
    len2 = u_strToLower(NULL, 0, (UChar *)buf, -1, locale, &err);
    if (len2 > len) {
        buf = realloc(buf, len2+1);
    }
    err = U_ZERO_ERROR;
    u_strToLower(buf, len2+1, (UChar *)buf, -1, locale, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToLower failed %s: %s\n", s, u_errorName(err));
        free(buf);
        return NULL;
    }
    
    // lowered uchar to utf8
    err = U_ZERO_ERROR;
    u_strToUTF8(NULL, 0, &len, buf, -1, &err);
    buf2 = malloc(sizeof(char) * len+1);
    err = U_ZERO_ERROR;
    u_strToUTF8(buf2, len+1, &len, (UChar *)buf, -1, &err);
    if (U_FAILURE(err)) {
        fprintf(stderr, "u_strToUTF8 failed %s: %s\n", s, u_errorName(err));
        free(buf);
        free(buf2);
        return NULL;
    }
    free(buf);
    return buf2;
}

void put_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *key, *data, *ts, *locale;
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
    locale = (char *)evhttp_find_header(&args, "locale");
    if (!locale) {
        // "" for root locale, NULL for default
        locale = "";
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
            e->key = utf8_tolower(key, locale);
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
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    evhttp_clear_headers(&args);
    evbuffer_free(buf);
}

void del_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *locale, *qkey, *key = NULL;
    struct el *e;
    struct namespace *ns;

    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    if (!locale) {
        // "" for root locale, NULL for default
        locale = "";
    }
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }

    if (DEBUG) {
        fprintf(stderr, "/search_cb %s %s\n", namespace, key);
    }

    if (namespace && key) {
        ns = get_namespace(namespace);
        if (ns) {
            HASH_FIND_STR(ns->elems, key, e);
            if (e) {
                HASH_DEL(ns->elems, e);
                free_el(e);
                evhttp_send_reply(req, HTTP_OK, "OK", buf);
            } else {
                evhttp_send_reply(req, HTTP_NOTFOUND, "KEY_NOT_FOUND", buf);
            }
        } else {
            evhttp_send_reply(req, HTTP_NOTFOUND, "NAMESPACE_NOT_FOUND", buf);
        }
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    evhttp_clear_headers(&args);
    if (key) {
        free(key);
    }
    evbuffer_free(buf);
}

void incr_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *locale, *svalue, *qkey, *key = NULL;
    struct el *e;
    int value = 0;
    struct namespace *ns;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    if (!locale) {
        // "" for root locale, NULL for default
        locale = "";
    }
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }
    svalue = (char *)evhttp_find_header(&args, "value");
    if (svalue) {
        value = atoi(svalue);
        if (arg) {  // this is a decr
            value = -value;
        }
    }
    
    if (DEBUG) {
        fprintf(stderr, "/incr_cb %s %s\n", namespace, key);
    }
    
    if (namespace && key && value) {
        ns = get_namespace(namespace);
        if (ns) {
            HASH_FIND_STR(ns->elems, key, e);
            if (e) {
                e->count += value;
                if (e->count <= 0) {
                    HASH_DEL(ns->elems, e);
                    free_el(e);
                }
                evhttp_send_reply(req, HTTP_OK, "OK", buf);
            } else {
                evhttp_send_reply(req, HTTP_NOTFOUND, "KEY_NOT_FOUND", buf);
            }
        } else {
            evhttp_send_reply(req, HTTP_NOTFOUND, "NAMESPACE_NOT_FOUND", buf);            
        }
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }
    
    evhttp_clear_headers(&args);
    if (key) {
        free(key);
    }
    evbuffer_free(buf);
}

void search_cb(struct evhttp_request *req, void *arg)
{
    struct evbuffer *buf = evbuffer_new();
    struct evkeyvalq args;
    char *namespace, *slimit, *locale, *qkey, *key = NULL;
    struct json_object *jsobj, *jsel, *jsresults;
    struct el *e, *results = NULL;
    struct namespace *ns;
    int i, limit = 10;
    
    evhttp_parse_query(req->uri, &args);
    namespace = (char *)evhttp_find_header(&args, "namespace");
    locale = (char *)evhttp_find_header(&args, "locale");
    if (!locale) {
        // "" for root locale, NULL for default
        locale = "";
    }
    qkey = (char *)evhttp_find_header(&args, "key");
    if (qkey) {
        key = utf8_tolower(qkey, locale);
    }
    slimit = (char *)evhttp_find_header(&args, "limit");
    if (slimit) {
        limit = atoi(slimit);
    }

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
            for (e=results, i=0; e != NULL && i < limit; e=e->rh.next, i++) {
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
        evbuffer_add_printf(buf, "%s\n", (char *)json_object_to_json_string(jsobj));
        evhttp_send_reply(req, HTTP_OK, "OK", buf);
        json_object_put(jsobj);
    } else {
        evhttp_send_reply(req, HTTP_BADREQUEST, "MISSING_REQ_ARG", buf);
    }

    evhttp_clear_headers(&args);
    if (key) {
        free(key);
    }
    evbuffer_free(buf);
}

void termination_handler(int signum)
{
    event_loopbreak();
}

int main(int argc, char **argv)
{
    int opt;
    int port = 8080;
    char *address = "0.0.0.0";

    while((opt = getopt(argc, argv, "a:p:")) != -1) {
        switch(opt) {
        case 'a':
            address = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case '?':
            fprintf (stderr, "Unknown option '-%c'.\n", optopt);
            return 1;
        }
    }

    signal(SIGINT, termination_handler);
    signal(SIGQUIT, termination_handler);
    signal(SIGTERM, termination_handler);
    signal(SIGPIPE, SIG_IGN);

    fprintf(stdout, "%s (%s) listening on: %s:%d\n", NAME, VERSION, address, port);

    event_init();
    httpd = evhttp_start(address, port);

    evhttp_set_cb(httpd, "/put", put_cb, NULL);
    evhttp_set_cb(httpd, "/put", put_cb, NULL);
    evhttp_set_cb(httpd, "/del", del_cb, NULL);
    evhttp_set_cb(httpd, "/incr", incr_cb, NULL);
    evhttp_set_cb(httpd, "/decr", incr_cb, (void *)'d');
    evhttp_set_cb(httpd, "/search", search_cb, NULL);

    event_dispatch();
    evhttp_free(httpd);

    return 0;
}
