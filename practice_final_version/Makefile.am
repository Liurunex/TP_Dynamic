AM_CPPFLAGS = -I$(top_srcdir) -isystem $(top_srcdir)/3rd -isystem $(top_srcdir)/3rd/protobuf/src -isystem $(top_srcdir)/3rd/leveldb/include  -isystem $(top_srcdir)/3rd/curl/include

if ADS_ENABLE_DEBUG
if ADS_ENABLE_MACOSX
werror = -Wall -Wno-deprecated-declarations
else
werror = -Werror
endif
else
werror = -Wall
endif

AM_CXXFLAGS = $(werror) 

noinst_LIBRARIES = libforecast.a 
lib_LTLIBRARIES =

noinst_HEADERS = \
	Forecast_Adaptive_TP_Service.h \
	Forecast_Task.h \
	Forecast_Repository.h \
	Forecast_Store.h \
	Forecast_Metrics.h \
	Forecast_Collector.h \
	Forecast_Aggregator.h \
	Forecast_Planner.h \
	Forecast_Simulator.h \
	Forecast_Pusher.h \
	Forecast_Service.h \
	Forecast_Performance.h \
	True_Table_Conflict_Detector.h

libforecast_a_SOURCES = \
	Forecast_Adaptive_TP_Service.cpp \
	Forecast_Task.cpp \
	Forecast_Repository.cpp \
	Forecast_Store.cpp \
	Forecast_Metrics.cpp \
	Forecast_Collector.cpp \
	Forecast_Aggregator.cpp \
	Forecast_Planner.cpp \
	Forecast_Simulator.cpp \
	Forecast_Service.cpp\
	Forecast_Pusher.cpp \
	Forecast_Pusher_DB_View.cpp \
	Forecast_Performance.cpp \
	True_Table_Conflict_Detector.cpp

if ADS_ENABLE_TESTING
AM_CPPFLAGS += -I$(top_srcdir)/3rd/gmock -isystem $(top_srcdir)/3rd/gmock
AM_CPPFLAGS += -I$(top_srcdir)/3rd/gmock/cppfreemock -isystem $(top_srcdir)/3rd/gmock/cppfreemock
AM_CPPFLAGS += -DADS_ENABLE_TESTING
AM_CPPFLAGS += -fno-access-control
endif

if ADS_ENABLE_TESTING
# Move these files to server/Makefile.am INK-14683
# AM_CPPFLAGS += -I$(top_srcdir)/test/include -isystem $(top_srcdir)/test/include
# ads_LDADD += $(top_builddir)/3rd/gmock/libgmock.a
# libforecast_a_SOURCES += $(top_srcdir)/test/utils/Ads_Unittest.cpp
# libforecast_a_SOURCES += $(top_srcdir)/test/forecast/Forecast_Metrics.test.cpp
# libforecast_a_SOURCES += $(top_srcdir)/test/forecast/Forecast_Service.test.cpp
# libforecast_a_SOURCES += $(top_srcdir)/test/forecast/Forecast_Simulator.test.cpp
# libforecast_a_SOURCES += $(top_srcdir)/test/forecast/Forecast_Store.test.cpp
endif
