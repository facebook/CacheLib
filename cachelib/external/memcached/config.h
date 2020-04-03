/* config.h.  Generated from config.h.in by configure.  */
/* config.h.in.  Generated from configure.ac by autoheader.  */

/* Define if building universal (internal helper macro) */
/* #undef AC_APPLE_UNIVERSAL_BUILD */

/* Set to nonzero if you want to enable ARMv8 crc32 */
/* #undef ARM_CRC32 */

/* Set to nonzero if you want to compile using ASAN */
/* #undef ASAN */

/* Set to nonzero if you want to include DTRACE */
/* #undef ENABLE_DTRACE */

/* Set to nonzero if you want to include SASL */
/* #undef ENABLE_SASL */

/* Set to nonzero if you want to enable a SASL pwdb */
/* #undef ENABLE_SASL_PWDB */

/* machine is bigendian */
/* #undef ENDIAN_BIG */

/* machine is littleendian */
#define ENDIAN_LITTLE 1

/* Set to nonzero if you want to enable extstore */
#define EXTSTORE 1

/* Define to 1 if support accept4 */
#define HAVE_ACCEPT4 1

/* Define to 1 if you have the `cap_enter' function. */
/* #undef HAVE_CAP_ENTER */

/* Define to 1 if you have the `clock_gettime' function. */
#define HAVE_CLOCK_GETTIME 1

/* Define this if you have an implementation of drop_privileges() */
/* #undef HAVE_DROP_PRIVILEGES */

/* Define this if you have an implementation of drop_worker_privileges() */
/* #undef HAVE_DROP_WORKER_PRIVILEGES */

/* GCC 64bit Atomics available */
#define HAVE_GCC_64ATOMICS 1

/* GCC Atomics available */
#define HAVE_GCC_ATOMICS 1

/* Define to 1 if support getopt_long */
#define HAVE_GETOPT_LONG 1

/* Define to 1 if you have the `getpagesizes' function. */
#define HAVE_GETPAGESIZES 1

/* Have ntohll */
/* #undef HAVE_HTONLL */

/* Define to 1 if you have the <inttypes.h> header file. */
#define HAVE_INTTYPES_H 1

/* Define to 1 if you have the `memcntl' function. */
/* #undef HAVE_MEMCNTL */

/* Define to 1 if you have the <memory.h> header file. */
#define HAVE_MEMORY_H 1

/* Define to 1 if you have the `mlockall' function. */
#define HAVE_MLOCKALL 1

/* Define to 1 if you have the `pledge' function. */
/* #undef HAVE_PLEDGE */

/* we have sasl_callback_ft */
#define HAVE_SASL_CALLBACK_FT 1

/* Set to nonzero if your SASL implementation supports SASL_CB_GETCONF */
/* #undef HAVE_SASL_CB_GETCONF */

/* Set to nonzero if your SASL implementation supports SASL_CB_GETCONFPATH */
/* #undef HAVE_SASL_CB_GETCONFPATH */

/* Define to 1 if you have the <sasl/sasl.h> header file. */
#define HAVE_SASL_SASL_H 1

/* Define to 1 if you have the `setppriv' function. */
/* #undef HAVE_SETPPRIV */

/* Define to 1 if you have the `sigignore' function. */
#define HAVE_SIGIGNORE 1

/* Define to 1 if stdbool.h conforms to C99. */
#define HAVE_STDBOOL_H 1

/* Define to 1 if you have the <stdint.h> header file. */
#define HAVE_STDINT_H 1

/* Define to 1 if you have the <stdlib.h> header file. */
#define HAVE_STDLIB_H 1

/* Define to 1 if you have the <strings.h> header file. */
#define HAVE_STRINGS_H 1

/* Define to 1 if you have the <string.h> header file. */
#define HAVE_STRING_H 1

/* Define to 1 if you have the `sysconf' function. */
#define HAVE_SYSCONF 1

/* Define to 1 if you have the <sys/stat.h> header file. */
#define HAVE_SYS_STAT_H 1

/* Define to 1 if you have the <sys/types.h> header file. */
#define HAVE_SYS_TYPES_H 1

/* Define to 1 if you have the <unistd.h> header file. */
#define HAVE_UNISTD_H 1

/* Define to 1 if the system has the type `_Bool'. */
#define HAVE__BOOL 1

/* Machine need alignment */
/* #undef NEED_ALIGN */

/* Name of package */
#define PACKAGE "memcached"

/* Define to the address where bug reports for this package should be sent. */
#define PACKAGE_BUGREPORT "memcached@googlegroups.com"

/* Define to the full name of this package. */
#define PACKAGE_NAME "memcached"

/* Define to the full name and version of this package. */
#define PACKAGE_STRING "memcached UNKNOWN"

/* Define to the one symbol short name of this package. */
#define PACKAGE_TARNAME "memcached"

/* Define to the home page for this package. */
#define PACKAGE_URL ""

/* Define to the version of this package. */
#define PACKAGE_VERSION "UNKNOWN"

/* The size of `void *', as computed by sizeof. */
#define SIZEOF_VOID_P 8

/* Set to nonzero if you want to compile a statically linked binary */
/* #undef STATIC */

/* Define to 1 if you have the ANSI C header files. */
#define STDC_HEADERS 1

/* Set to nonzero if you want to enable TLS */
/* #undef TLS */

/* Version number of package */
#define VERSION "UNKNOWN"

/* Define WORDS_BIGENDIAN to 1 if your processor stores words with the most
   significant byte first (like Motorola and SPARC, unlike Intel). */
#if defined AC_APPLE_UNIVERSAL_BUILD
# if defined __BIG_ENDIAN__
#  define WORDS_BIGENDIAN 1
# endif
#else
# ifndef WORDS_BIGENDIAN
/* #  undef WORDS_BIGENDIAN */
# endif
#endif

/* find sigignore on Linux */
#define _GNU_SOURCE 1

/* Define to empty if `const' does not conform to ANSI C. */
/* #undef const */

/* define to int if socklen_t not available */
/* #undef socklen_t */

#if HAVE_STDBOOL_H
#include <stdbool.h>
#else
#define bool char
#define false 0
#define true 1
#endif 

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

