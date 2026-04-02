package com.rydytrader.autotrader.service;

import com.rydytrader.autotrader.entity.AppUser;
import com.rydytrader.autotrader.repository.AppUserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AppUserService implements UserDetailsService {

    private static final Logger log = LoggerFactory.getLogger(AppUserService.class);

    private final AppUserRepository userRepo;
    private final PasswordEncoder passwordEncoder;

    public AppUserService(AppUserRepository userRepo, PasswordEncoder passwordEncoder) {
        this.userRepo = userRepo;
        this.passwordEncoder = passwordEncoder;
    }

    @jakarta.annotation.PostConstruct
    public void seedDefaultUsers() {
        if (userRepo.count() > 0) return;

        userRepo.save(new AppUser("rydytrader@gmail.com",
            passwordEncoder.encode("Ryan2004#"), "ROLE_ADMIN", "Prabhu", "Arumugham"));
        userRepo.save(new AppUser("tcsprabhu@gmail.com",
            passwordEncoder.encode("password"), "ROLE_VIEWER", "Prabhu", "TCS"));

        log.info("[AppUserService] Seeded 2 default users (ADMIN + VIEWER)");
    }

    @Override
    public UserDetails loadUserByUsername(String email) throws UsernameNotFoundException {
        AppUser user = userRepo.findByEmail(email)
            .orElseThrow(() -> new UsernameNotFoundException("User not found: " + email));

        return new User(user.getEmail(), user.getPassword(),
            List.of(new SimpleGrantedAuthority(user.getRole())));
    }
}
