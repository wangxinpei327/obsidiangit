找到问题根源了。分析结果如下：

  ---
  根因分析

  漏洞在 KyuubiRestFrontendService.scala:108，认证过滤器只覆盖了 /v1/* 路径：

  // 第108行 — 过滤器只保护了 /v1/*，漏掉了 openapi 端点
  contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))

  暴露的两个不安全端点：
  - /api/openapi.json — OpenAPI 规范文档（由 KyuubiOpenApiResource 提供）
  - /api/openapi.yaml — YAML 格式的规范文档
  - /ui/swagger — Swagger UI 页面（Web UI 中内嵌的 swagger-ui-dist）

  这些路径不在 /v1/* 下，完全绕过认证。

  ---
  修复方案（按速度排序）

  方案一：关闭 REST 前端（最快，无需改代码）

  如果业务不需要 REST API（仅用 Thrift JDBC），在 conf/kyuubi-defaults.conf 中关闭：

  kyuubi.frontend.protocols=THRIFT_BINARY

  重启服务即可。10099 端口不再监听，漏洞立消。

  ---
  方案二：改一行代码，让过滤器覆盖所有路径（保留 REST API 功能）

  修改 kyuubi-server/src/main/scala/org/apache/kyuubi/server/KyuubiRestFrontendService.scala 第 108 行：

  // 改前
  contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))

  // 改后 — 覆盖 /api/ 下的所有路径
  contextHandler.addFilter(holder, "/*", EnumSet.allOf(classOf[DispatcherType]))

  然后重新编译部署：
  build/mvn -Pfast clean package -pl kyuubi-server -am -DskipTests

  这样 /api/openapi.json、/api/openapi.yaml 甚至 /api/ping 等端点都会需要认证。如果想让 ping 保持免认证，可以用方案三。

  ---
  方案三：单独为 OpenAPI 路径加过滤器（保留 ping 等免认证）

  // 原有的 v1 保护
  contextHandler.addFilter(holder, "/v1/*", EnumSet.allOf(classOf[DispatcherType]))
  // 再加一行保护 openapi
  contextHandler.addFilter(holder, "/openapi.*", EnumSet.allOf(classOf[DispatcherType]))

  ---
  方案四：如果不用 Swagger UI，可跳过 Web UI 构建

  Web UI 中内嵌了 Swagger UI 页面（/ui/swagger），如果只关 UI 不影响 REST API：

  # 构建时跳过 Web UI
  build/mvn -Dwebui.skip=true clean package -pl kyuubi-server -am -DskipTests

  ---
  推荐：如果 REST API 本身也不必要，选方案一（改配置重启，30 秒搞定）。如果需要保留 REST API，选方案二（改一行代码，最快路径）。
