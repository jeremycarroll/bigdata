require 'formula'

class Bigdata < Formula
  homepage 'http://bigdata.com/blog/'
  url 'http://bigdata.com/deploy/bigdata-1.3.0.tgz'
  sha1 '466cbce9241e3d418a53a88d81b108f42f4e9f4a'

  def install
    prefix.install Dir['*']

    # Set the installation path as the root for the bin scripts:
    system "sed -i .bk 's|<%= BD_HOME %>|#{prefix}|' #{bin}/bigdata"

    
    # Set the Jetty root as the resourceBase in the jetty.xml file:
    system "sed -i .bk 's|<%= JETTY_DIR %>|#{prefix}/var/jetty|' #{prefix}/var/jetty/etc/jetty.xml"

    # Set the installation path as the root for bigdata.jnl file location (<bigdata_home>/data):
    system "sed -i .bk 's|<%= BD_HOME %>|#{prefix}|' #{prefix}/var/jetty/WEB-INF/RWStore.properties"

    # Set the installation path as the root for log files (<bigdata_home>/log):
    system "sed -i .bk 's|<%= BD_HOME %>|#{prefix}|' #{prefix}/var/jetty/WEB-INF/classes/log4j.properties"
  end

  def caveats; <<-EOS.undent
     After launching, visit the Bigdata Workbench at: 

       http://localhost:8080/bigdata

     "bigdata" command synopis:
     -------------------------

     Start the server:

          % bigdata start

     Stop the server:

          % bigdata stop

     Restart the server:

          % bigdata restart

     To tune the server configuration, edit the "#{prefix}/var/jetty/WEB-INF/RWStore.properties" file.

     Further documentation:
	
          #{prefix}/doc
    EOS
  end

  plist_options :startup => 'true', :manual => 'bigdata start'

  def plist; <<-EOS.undent
    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple Computer//DTD PLIST 1.0//EN"
    "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
      <dict>
        <key>Label</key>
        <string>#{plist_name}</string>
        <key>Program</key>
        <string>#{bin}/bigdata</string>
        <key>RunAtLoad</key>
        <true/>
        <key>WorkingDirectory</key>
        <string>#{prefix}</string>
      </dict>
    </plist>
    EOS
  end

end
