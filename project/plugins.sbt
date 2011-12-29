resolvers += Resolver.url("heikoseeberger", new java.net.URL("http://hseeberger.github.com/releases"))(Resolver.ivyStylePatterns)

addSbtPlugin("name.heikoseeberger.sbtsrc" % "sbtsrc" % "1.1.0")

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse" % "1.5.0")
