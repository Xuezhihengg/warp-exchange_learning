package org.warpexchange_learning.ui.ui.web;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;
import org.warpexchange_learning.common.ApiException;
import org.warpexchange_learning.common.bean.AuthToken;
import org.warpexchange_learning.common.bean.TransferRequestBean;
import org.warpexchange_learning.common.client.RestClient;
import org.warpexchange_learning.common.ctx.UserContext;
import org.warpexchange_learning.common.enums.AssetEnum;
import org.warpexchange_learning.common.enums.UserType;
import org.warpexchange_learning.common.model.ui.UserProfileEntity;
import org.warpexchange_learning.common.support.LoggerSupport;
import org.warpexchange_learning.common.user.UserService;
import org.warpexchange_learning.common.util.HashUtil;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.regex.Pattern;


/**
 * 为什么要把UI模块和API模块分开:
 * API和Web页面不同，Web页面可以给用户一个登录页，登录成功后设置Session或Cookie，后续请求检查的是Session或Cookie。
 * API不能使用Session，因为Session很难做无状态集群，API也不建议使用Cookie，因为API域名很可能与Web UI的域名不一致，拿不到Cookie。
 */
@Component
public class MvcController extends LoggerSupport {

    @Value("#{exchangeConfiguration.hmacKey}")
    String hmacKey;

    @Autowired
    CookieService cookieService;

    @Autowired
    UserService userService;

    @Autowired
    RestClient restClient;

    @Autowired
    Environment environment;

    @PostConstruct
    public void init() {
        // 本地开发环境下自动创建用户user0@example.com ~ user9@example.com:
        if (isLocalDevEnv()) {
            for (int i = 0; i <= 9; i++) {
                String email = "user" + i + "@example.com";
                String name = "User-" + i;
                String password = "password" + i;
                if (userService.fetchUserProfileByEmail(email) == null) {
                    logger.info("auto create user {} for local dev env...", email);
                    doSignup(email, name, password);
                }
            }
        }
    }

    /**
     * Index page.
     */
    @GetMapping("/")
    public ModelAndView index() {
        if (UserContext.getUserId() == null) {
            return redirect("/signin");
        }
        return prepareModelAndView("/index");
    }

    @GetMapping("/signup")
    public ModelAndView signup() {
        if (UserContext.getUserId() != null) {
            return redirect("/");
        }
        return prepareModelAndView("signup");
    }

    @PostMapping("/signup")
    public ModelAndView signup(@RequestParam("email") String email, @RequestParam("name") String name, @RequestParam("password") String password) {
        // check email:
        if (email == null || email.isBlank()) {
            return prepareModelAndView("signup", Map.of("email", email, "name", name, "error", "Invalid email."));
        }
        email = email.strip().toLowerCase();
        if (email.length() > 100 || !EMAIL.matcher(email).matches()) {
            return prepareModelAndView("signup", Map.of("email", email, "name", name, "error", "Invalid email."));
        }
        if (userService.fetchUserProfileByEmail(email) != null) {
            return prepareModelAndView("signup", Map.of("email", email, "name", name, "error", "Email exists."));
        }
        // check name:
        if (name == null || name.isBlank() || name.strip().length() > 100) {
            return prepareModelAndView("signup", Map.of("email", email, "name", name, "error", "Invalid name."));
        }
        name = name.strip();
        // check password:
        if (password == null || password.length() < 8 || password.length() > 32) {
            return prepareModelAndView("signup", Map.of("email", email, "name", name, "error", "Invalid password."));
        }
        doSignup(email, name, password);
        return redirect("/signin");
    }

    @PostMapping(value = "/websocket/token", produces = "application/json")
    @ResponseBody
    String requestWebSocketToken() {
        Long userId = UserContext.getUserId();
        if (userId == null) {
            // 无登录信息，返回JSON空字符串"":
            return "\"\"";
        }
        // 1分钟后过期:
        AuthToken token = new AuthToken(userId, System.currentTimeMillis() + 60_000);
        String strToken = token.toSecureString(hmacKey);
        // 返回JSON字符串"xxx":
        return "\"" + strToken + "\"";
    }

    @GetMapping("/signin")
    public ModelAndView signin(HttpServletRequest request) {
        if (UserContext.getUserId() != null) {
            return redirect("/");
        }
        return prepareModelAndView("signin");
    }

    /**
     * Do sign in.
     */
    @PostMapping("/signin")
    public ModelAndView signIn(@RequestParam("email") String email, @RequestParam("password") String password, HttpServletRequest request, HttpServletResponse response) {
        if (email == null || email.isEmpty()) {
            return prepareModelAndView("signin", Map.of("email", email, "error", "Invalid email or password."));
        }
        if (password == null || password.isEmpty()) {
            return prepareModelAndView("signin", Map.of("email", email, "error", "Invalid email or password."));
        }
        email = email.toLowerCase();
        try {
            UserProfileEntity userProfile = userService.signin(email, password);
            // sign in ok and set cookie:
            AuthToken token = new AuthToken(userProfile.userId,
                    System.currentTimeMillis() + 1000 * cookieService.getExpiresInSeconds());
            cookieService.setSessionCookie(request, response, token);
        } catch (ApiException e) {
            logger.warn("sign in failed for " + e.getMessage(), e);
            return prepareModelAndView("signin", Map.of("email", email, "error", "Invalid email or password."));
        } catch (Exception e) {
            logger.warn("sign in failed for " + e.getMessage(), e);
            return prepareModelAndView("signin", Map.of("email", email, "error", "Internal server error."));
        }

        logger.info("signin ok.");
        return redirect("/");
    }

    @GetMapping("/signout")
    public ModelAndView signout(HttpServletRequest request, HttpServletResponse response) {
        cookieService.deleteSessionCookie(request, response);
        return redirect("/");
    }



    // util method:


    private UserProfileEntity doSignup(String email, String name, String password) {
        // sign up 就是向数据库中添加UserProfileEntity:
        UserProfileEntity profile = userService.signup(email, name, password);
        // 本地开发环境下自动给用户增加资产:
        if (isLocalDevEnv()) {
            logger.warn("auto deposit assets for user {} in local dev env...", profile.email);
            Random random = new Random(profile.userId);
            deposit(profile.userId, AssetEnum.BTC, new BigDecimal(random.nextInt(5_00, 10_00)).movePointLeft(2));
            deposit(profile.userId, AssetEnum.USD, new BigDecimal(random.nextInt(100000_00, 400000_00)).movePointLeft(2));
        }
        logger.info("user signed up: {}", profile);
        return profile;
    }

    private boolean isLocalDevEnv() {
        return environment.getActiveProfiles().length == 0
                && Arrays.equals(environment.getDefaultProfiles(), new String[] { "default" });
    }

    private void deposit(Long userId, AssetEnum asset, BigDecimal amount) {
        var req = new TransferRequestBean();
        req.transferId = HashUtil.sha256(userId + "/" + asset + "/" + amount.stripTrailingZeros().toPlainString())
                .substring(0, 32);
        req.amount = amount;
        req.asset = asset;
        // 由系统负债用户向userId转移资产
        req.fromUserId = UserType.DEBT.getInternalUserId();
        req.toUserId = userId;
        restClient.post(Map.class, "/internal/transfer", null, req);
    }

    ModelAndView prepareModelAndView(String view, Map<String, Object> model) {
        ModelAndView mv = new ModelAndView(view);
        mv.addAllObjects(model);
        addGlobalModel(mv);
        return mv;
    }

    ModelAndView prepareModelAndView(String view) {
        ModelAndView mv = new ModelAndView(view);
        addGlobalModel(mv);
        return mv;
    }

    ModelAndView prepareModelAndView(String view, String key, Object value) {
        ModelAndView mv = new ModelAndView(view);
        mv.addObject(key, value);
        addGlobalModel(mv);
        return mv;
    }

    void addGlobalModel(ModelAndView mv) {
        final Long userId = UserContext.getUserId();
        mv.addObject("__userId__", userId);
        mv.addObject("__profile__", userId == null ? null : userService.getUserProfile(userId));
        mv.addObject("__time__", Long.valueOf(System.currentTimeMillis()));
    }

    ModelAndView notFound() {
        ModelAndView mv = new ModelAndView("404");
        addGlobalModel(mv);
        return mv;
    }

    ModelAndView redirect(String url) {
        return new ModelAndView("redirect:" + url);
    }

    static Pattern EMAIL = Pattern.compile("^[a-z0-9\\-\\.]+\\@([a-z0-9\\-]+\\.){1,3}[a-z]{2,20}$");
}
