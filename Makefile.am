# Copyright (C) 2008 Hiroaki Kawai

pkgplugindir=@MYSQL_PLUGINDIR@
INCLUDES= -I$(top_builddir)/include -I$(top_srcdir)/include @MYSQL_INCLUDES@
#noinst_LTLIBRARIES= mypluglib.la
pkgplugin_LTLIBRARIES= libftsuffix.la
libftsuffix_la_SOURCES= ftnorm.c ftbool.c plugin_suffix.c
libftsuffix_la_LDFLAGS= -module -rpath $(pkgplugindir)
libftsuffix_la_CFLAGS= -DMYSQL_DYNAMIC_PLUGIN
