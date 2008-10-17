#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#include "ftbool.h"
#if HAVE_ICU
#include <unicode/uclean.h>
#include <unicode/uversion.h>
#include <unicode/uchar.h>
#include <unicode/unorm.h>
#include "ftnorm.h"
#endif

// mysql headers
#include <my_global.h>
#include <m_ctype.h>
#include <my_sys.h>
#include <plugin.h>
/// #include <ft_global.h>
#define HA_FT_MAXBYTELEN 254

#if !defined(__attribute__) && (defined(__cplusplus) || !defined(__GNUC__)  || __GNUC__ == 2 && __GNUC_MINOR__ < 8)
#define __attribute__(A)
#endif

static char* suffix_unicode_normalize="OFF";
static char* suffix_unicode_version="DEFAULT";
static char icu_unicode_version[32];

static void* icu_malloc(const void* context, size_t size){ return my_malloc(size,MYF(MY_WME)); }
static void* icu_realloc(const void* context, void* ptr, size_t size){ return my_realloc(ptr,size,MYF(MY_WME)); }
static void  icu_free(const void* context, void *ptr){ my_free(ptr,MYF(0)); }

static int suffix_parser_plugin_init(void *arg __attribute__((unused))){
#if HAVE_ICU
  char errstr[128];
  UVersionInfo versionInfo;
  u_getUnicodeVersion(versionInfo);
  u_versionToString(versionInfo, icu_unicode_version);
  
  UErrorCode ustatus=0;
  u_setMemoryFunctions(NULL, icu_malloc, icu_realloc, icu_free, &ustatus);
  if(U_FAILURE(ustatus)){
    sprintf(errstr, "u_setMemoryFunctions failed. ICU status code %d\n", ustatus);
    fputs(errstr, stderr);
    fflush(stderr);
  }
#endif
  return(0);
}

static int suffix_parser_plugin_deinit(void *arg __attribute__((unused))){
  return(0);
}


static int suffix_parser_init(MYSQL_FTPARSER_PARAM *param __attribute__((unused))){
  return(0);
}
static int suffix_parser_deinit(MYSQL_FTPARSER_PARAM *param __attribute__((unused))){
  return(0);
}

static size_t str_convert(CHARSET_INFO *cs, char *from, size_t from_length,
                          CHARSET_INFO *uc, char *to,   size_t to_length){
  char *rpos, *rend, *wpos, *wend;
  my_wc_t wc;
  
  rpos = from;
  rend = from + from_length;
  wpos = to;
  wend = to + to_length;
  while(rpos < rend){
    int cnvres = 0;
    cnvres = cs->cset->mb_wc(cs, &wc, (uchar*)rpos, (uchar*)rend);
    if(cnvres > 0){
      rpos += cnvres;
    }else if(cnvres == MY_CS_ILSEQ){
      rpos++;
      wc = '?';
    }else{
      break;
    }
    cnvres = uc->cset->wc_mb(uc, wc, (uchar*)wpos, (uchar*)wend);
    if(cnvres > 0){
      wpos += cnvres;
    }else{
      break;
    }
  }
  return (size_t)(wpos - to);
}

static void suffix_add_word(MYSQL_FTPARSER_PARAM *param, char* buff, size_t length, MYSQL_FTPARSER_BOOLEAN_INFO* instinfo, int skip){
  char* pos=buff;
  char* docend=buff+length;
  while(pos < docend){
    // we must not exceed HA_FT_MAXBYTELEN-HA_FT_WLEN
    if(docend-pos > HA_FT_MAXBYTELEN){
      char* c = pos;
      while(c < pos+HA_FT_MAXBYTELEN){
        int len = param->cs->cset->mbcharlen(param->cs, (uint)(*c));
        if(c+len > pos+HA_FT_MAXBYTELEN) break;
        
        if(len > 0){
          c += len;
        }else{ // illegal sequence.
          c++;
        }
      }
      
      param->mysql_add_word(param, pos, c-pos, instinfo);
char bus[1024];
memcpy(bus,pos,c-pos);
bus[c-pos]='\0';
fputs(bus,stderr);
fflush(stderr);
      if(skip){
        pos = c;
      }else{
        int len = param->cs->cset->mbcharlen(param->cs, (uint)*pos);
        if(len > 0){
          pos += len;
        }else{
          pos++;
        }
      }
    }else{
      param->mysql_add_word(param, pos, docend-pos, instinfo);
char bus[1024];
memcpy(bus,pos,docend-pos);
bus[docend-pos]='\0';
fputs(bus,stderr);
fflush(stderr);
      if(skip) break;
      
      int len = param->cs->cset->mbcharlen(param->cs, (uint)*pos);
      if(len > 0){
        pos += len;
      }else{
        pos++;
      }
    }
  }
}

static int suffix_parser_parse(MYSQL_FTPARSER_PARAM *param)
{
  DBUG_ENTER("suffix_parser_parse");
  
  CHARSET_INFO *uc = NULL;
  CHARSET_INFO *cs = param->cs;
  char* feed = param->doc;
  size_t feed_length = (size_t)param->length;
  int feed_req_free = 0;
  
  // we do convert if it was requred to normalize.
  if(strcmp(cs->csname, "utf8")!=0 && strcmp(suffix_unicode_normalize, "OFF")!=0){
    uc = get_charset(33,MYF(0)); // my_charset_utf8_general_ci for utf8 conversion
  }
  
  // convert into UTF-8
  if(uc){
    char* cv;
    size_t cv_length=0;
    // calculate mblen and malloc.
    cv_length = uc->mbmaxlen * cs->cset->numchars(cs, feed, feed+feed_length);
    cv = my_malloc(cv_length, MYF(MY_WME));
    feed_length = str_convert(cs, feed, feed_length, uc, cv, cv_length);
    feed = cv;
    feed_req_free = 1;
  }
  
#if HAVE_ICU
  // normalize
  if(strcmp(suffix_unicode_normalize, "OFF")!=0){
    char* nm;
    char* t;
    size_t nm_length=0;
    size_t nm_used=0;
    nm_length = feed_length+32;
    nm = my_malloc(nm_length, MYF(MY_WME));
    int status = 0;
    int mode = UNORM_NONE;
    int options = 0;
    if(strcmp(suffix_unicode_normalize, "C")==0) mode = UNORM_NFC;
    if(strcmp(suffix_unicode_normalize, "D")==0) mode = UNORM_NFD;
    if(strcmp(suffix_unicode_normalize, "KC")==0) mode = UNORM_NFKC;
    if(strcmp(suffix_unicode_normalize, "KD")==0) mode = UNORM_NFKD;
    if(strcmp(suffix_unicode_normalize, "FCD")==0) mode = UNORM_FCD;
    if(strcmp(suffix_unicode_version, "3.2")==0) options |= UNORM_UNICODE_3_2;
    t = uni_normalize(feed, feed_length, nm, nm_length, &nm_used, mode, options, &status);
    if(status != 0){
      nm_length=nm_used;
      nm = my_realloc(nm, nm_length, MYF(MY_WME));
      t = uni_normalize(feed, feed_length, nm, nm_length, &nm_used, mode, options, &status);
      if(status != 0){
        fputs("unicode normalization failed.\n",stderr);
        fflush(stderr);
      }else{
        nm = t;
      }
    }else{
      nm = t;
    }
    feed_length = nm_used;
    if(feed_req_free) my_free(feed,MYF(0));
    feed = nm;
    feed_req_free = 1;
  }
#endif
  
  if(uc){
    // convert from UTF-8
    int cv_length = cs->mbmaxlen * uc->cset->numchars(uc, feed, feed+feed_length);
    char* cv = my_malloc(cv_length, MYF(MY_WME));
    feed_length = str_convert(uc, feed, feed_length, cs, cv, cv_length);
    if(feed_req_free) my_free(feed,MYF(0));
    feed = cv;
    feed_req_free = 1;
  }
  
  if(feed_req_free){
    param->flags |= MYSQL_FTFLAGS_NEED_COPY;
  }
  if(param->mode == MYSQL_FTPARSER_FULL_BOOLEAN_INFO){
    // buffer is to be free-ed
    param->flags |= MYSQL_FTFLAGS_NEED_COPY;
    size_t talloc = 32;
    size_t tlen = 0;
    char*  tbuffer = my_malloc(talloc, MYF(MY_WME));
    
    // always trunc = 1;
    MYSQL_FTPARSER_BOOLEAN_INFO bool_info_may    ={ FT_TOKEN_WORD, 0, 0, 0, 1, ' ', 0 };
    MYSQL_FTPARSER_BOOLEAN_INFO instinfo;
    int depth=0;
    MYSQL_FTPARSER_BOOLEAN_INFO baseinfos[16];
    instinfo = baseinfos[0] = bool_info_may;
    
    int context=CTX_CONTROL;
    SEQFLOW sf,sf_prev = SF_BROKEN;
    char *pos=feed;
    while(pos < feed+feed_length){
      int readsize;
      my_wc_t dst;
      sf = ctxscan(cs, pos, feed+feed_length, &dst, &readsize, context);
      if(sf==SF_ESCAPE){
        context |= CTX_ESCAPE;
        context |= CTX_CONTROL;
      }else{
        context &= ~CTX_ESCAPE;
        if(sf == SF_CHAR){
          context &= ~CTX_CONTROL;
        }else{
          context |= CTX_CONTROL;
        }
        if(sf == SF_PLUS){
          instinfo.yesno = 1;
        }
        if(sf == SF_MINUS){
          instinfo.yesno = -1;
        }
        if(sf == SF_PLUS) instinfo.weight_adjust = 1;
        if(sf == SF_MINUS) instinfo.weight_adjust = -1;
        if(sf == SF_WASIGN){
          instinfo.wasign = -1;
        }
        if(sf == SF_LEFT_PAREN){
          depth++;
          if(depth>16) depth=16;
          baseinfos[depth] = instinfo;
          instinfo.type = FT_TOKEN_LEFT_PAREN;
          param->mysql_add_word(param, pos, 0, &instinfo); // push LEFT_PAREN token
          instinfo = baseinfos[depth];
        }
        if(sf == SF_RIGHT_PAREN){
          instinfo.type = FT_TOKEN_LEFT_PAREN;
          param->mysql_add_word(param, pos, 0, &instinfo); // push RIGHT_PAREN token
          depth--;
          if(depth<0) depth=0;
          instinfo = baseinfos[depth];
        }
        if(sf == SF_QUOTE_START){
          context |= CTX_QUOTE;
        }
        if(sf == SF_WHITE || sf == SF_QUOTE_END || sf == SF_LEFT_PAREN || sf == SF_RIGHT_PAREN || sf == SF_TRUNC){
          if(sf_prev == SF_CHAR){
            if(tlen>0){
              suffix_add_word(param, tbuffer, tlen, &instinfo, 1); // emit
            }
            tlen = 0;
            instinfo = baseinfos[depth];
          }
        }
        if(sf == SF_QUOTE_END){
          context &= ~CTX_QUOTE;
        }
        
        if(sf == SF_CHAR){
          if(tlen+readsize>talloc){
            talloc=tlen+readsize;
            tbuffer=my_realloc(tbuffer, talloc, MYF(MY_WME));
          }
          memcpy(tbuffer+tlen, pos, readsize);
          tlen += readsize;
        }else if(sf != SF_ESCAPE){
          tlen = 0;
        }
      }
      
      if(readsize > 0){
        pos += readsize;
      }else if(readsize == MY_CS_ILSEQ){
        pos++;
      }else{
        break;
      }
      sf_prev = sf;
    }
    if(sf==SF_CHAR){
      if(tlen>0){ // we must not exceed HA_FT_MAXBYTELEN-HA_FT_WLEN
        suffix_add_word(param, tbuffer, tlen, &instinfo, 1); // emit
      }
    }
    my_free(tbuffer, MYF(0)); // free-ed in deinit
  }else if(param->mode==MYSQL_FTPARSER_WITH_STOPWORDS){
    // phrase query.
    suffix_add_word(param, feed, feed_length, NULL, 1);
  }else{
    // Natural mode query / Indexing
    // The tokens are too much for natural mode query.
    suffix_add_word(param, feed, feed_length, NULL, 0);
  }
  if(feed_req_free) my_free(feed,MYF(0));
  
  DBUG_RETURN(0);
}

int suffix_unicode_version_check(MYSQL_THD thd, struct st_mysql_sys_var *var, void *save, struct st_mysql_value *value){
    char buf[4];
    int len=4;
    const char *str;
    
    str = value->val_str(value,buf,&len);
    if(!str) return -1;
    *(const char**)save=str;
    if(len==3){
      if(memcmp(str, "3.2", len)==0) return 0;
    }
    if(len==7){
      if(memcmp(str, "DEFAULT", len)==0) return 0;
    }
    return -1;
}

int suffix_unicode_normalize_check(MYSQL_THD thd, struct st_mysql_sys_var *var, void *save, struct st_mysql_value *value){
    char buf[4];
    int len=4;
    const char *str;
    
    str = value->val_str(value,buf,&len);
    if(!str) return -1;
    *(const char**)save=str;
    if(!get_charset(33,MYF(0))) return -1; // If you don't have utf8 codec in mysql, it fails
    if(len==1){
        if(str[0]=='C'){ return 0;}
        if(str[0]=='D'){ return 0;}
    }
    if(len==2){
        if(str[0]=='K' && str[1]=='C'){ return 0;}
        if(str[0]=='K' && str[1]=='D'){ return 0;}
    }
    if(len==3){
        if(str[0]=='F' && str[1]=='C' && str[2]=='D'){ return 0;}
        if(str[0]=='O' && str[1]=='F' && str[2]=='F'){ return 0;}
    }
    return -1;
}

static MYSQL_SYSVAR_STR(normalization, suffix_unicode_normalize,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "Set unicode normalization (OFF, C, D, KC, KD, FCD)",
  suffix_unicode_normalize_check, NULL, "OFF");

static MYSQL_SYSVAR_STR(unicode_version, suffix_unicode_version,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
  "Set unicode version (3.2, DEFAULT)",
  suffix_unicode_version_check, NULL, "DEFAULT");

static struct st_mysql_show_var suffix_status[]=
{
  {"ICU_unicode_version", (char *)icu_unicode_version, SHOW_CHAR},
  {0,0,0}
};

static struct st_mysql_sys_var* suffix_system_variables[]= {
#if HAVE_ICU
  MYSQL_SYSVAR(normalization),
  MYSQL_SYSVAR(unicode_version),
#endif
  NULL
};

static struct st_mysql_ftparser suffix_parser_descriptor=
{
  MYSQL_FTPARSER_INTERFACE_VERSION, /* interface version      */
  suffix_parser_parse,              /* parsing function       */
  suffix_parser_init,               /* parser init function   */
  suffix_parser_deinit              /* parser deinit function */
};

mysql_declare_plugin(ft_suffix)
{
  MYSQL_FTPARSER_PLUGIN,      /* type                            */
  &suffix_parser_descriptor,  /* descriptor                      */
  "suffix",                   /* name                            */
  "Hiroaki Kawai",            /* author                          */
  "suffix Full-Text Parser", /* description                     */
  PLUGIN_LICENSE_BSD,
  suffix_parser_plugin_init,  /* init function (when loaded)     */
  suffix_parser_plugin_deinit,/* deinit function (when unloaded) */
  0x0014,                     /* version                         */
  suffix_status,               /* status variables                */
  suffix_system_variables,     /* system variables                */
  NULL
}
mysql_declare_plugin_end;

