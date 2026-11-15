export default {
  fetch(request, env) {
    const url = new URL(request.url);
    if (url.hostname === "giabom-blog.pages.dev") {
      url.protocol = "https:";
      url.host = "giabom.online";
      return Response.redirect(url, 301);
    }
    return env.ASSETS.fetch(request);
  },
};
