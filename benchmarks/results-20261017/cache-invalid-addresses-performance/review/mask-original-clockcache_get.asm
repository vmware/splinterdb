
.cache/invalid-address-perf-20261017/builds/mask-original/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000053c60 <clockcache_get>:
   53c60:	push   %r15
   53c62:	lea    -0x1(%rcx),%eax
   53c65:	push   %r14
   53c67:	push   %r13
   53c69:	push   %r12
   53c6b:	push   %rbp
   53c6c:	push   %rbx
   53c6d:	mov    %rsi,%rbx
   53c70:	sub    $0x38,%rsp
   53c74:	mov    %ecx,0xc(%rsp)
   53c78:	mov    %r8,0x10(%rsp)
   53c7d:	cmp    $0x7,%eax
   53c80:	ja     542a7 <clockcache_get+0x647>
   53c86:	mov    0x8(%rdi),%rax
   53c8a:	mov    %edx,%esi
   53c8c:	mov    0x8(%rax),%rdx
   53c90:	mov    0x8(%rdx),%rdx
   53c94:	lea    -0x1(%rdx),%rcx
   53c98:	test   %rbx,%rcx
   53c9b:	jne    53e80 <clockcache_get+0x220>
   53ca1:	mov    %esi,0x8(%rsp)
   53ca5:	mov    %rdi,%r13
   53ca8:	movabs $0xcccccccccccccccd,%rbp
   53cb2:	data16 cs nopw 0x0(%rax,%rax,1)
   53cbd:	nopl   (%rax)
   53cc0:	mov    0x120(%rax),%rcx
   53cc7:	mov    %rbx,%rax
   53cca:	shr    %cl,%rax
   53ccd:	mov    %rax,%rcx
   53cd0:	mov    0x28(%r13),%rax
   53cd4:	shr    $0x2,%rax
   53cd8:	cmp    %rax,%rcx
   53cdb:	jae    53f70 <clockcache_get+0x310>
   53ce1:	mov    0x30(%r13),%rax
   53ce5:	mov    (%rax,%rcx,4),%r14d
   53ce9:	cmp    $0xffffffff,%r14d
   53ced:	je     53eb8 <clockcache_get+0x258>
   53cf3:	data16 lea 0x822ad(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53cfb:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53d03:	mov    0x8(%rsp),%r12d
   53d08:	mov    %r14d,%esi
   53d0b:	mov    %r13,%rdi
   53d0e:	mov    (%rax),%rax
   53d11:	mov    %rax,(%rsp)
   53d15:	test   %r12d,%r12d
   53d18:	je     53e50 <clockcache_get+0x1f0>
   53d1e:	call   aa4b0 <clockcache_try_get_read.constprop.0>
   53d23:	cmp    $0x1,%eax
   53d26:	jne    53d6e <clockcache_get+0x10e>
   53d28:	mov    $0x1,%r15d
   53d2e:	mov    %r15,%rax
   53d31:	mul    %rbp
   53d34:	xor    %eax,%eax
   53d36:	shr    $0x2,%rdx
   53d3a:	mov    %rdx,%r12
   53d3d:	nopl   (%rax)
   53d40:	pause
   53d42:	mov    %rax,%rcx
   53d45:	add    $0x1,%rax
   53d49:	cmp    %r12,%rcx
   53d4c:	jne    53d40 <clockcache_get+0xe0>
   53d4e:	cmp    $0x400,%r15
   53d55:	ja     53e28 <clockcache_get+0x1c8>
   53d5b:	mov    %r14d,%esi
   53d5e:	mov    %r13,%rdi
   53d61:	add    %r15,%r15
   53d64:	call   aa4b0 <clockcache_try_get_read.constprop.0>
   53d69:	cmp    $0x1,%eax
   53d6c:	je     53d2e <clockcache_get+0xce>
   53d6e:	test   %eax,%eax
   53d70:	jne    53e67 <clockcache_get+0x207>
   53d76:	mov    %r14d,%eax
   53d79:	mov    0x48(%r13),%rcx
   53d7d:	mov    %rax,%rdx
   53d80:	shl    $0x6,%rdx
   53d84:	mov    0x8(%rcx,%rdx,1),%rsi
   53d89:	cmp    %rsi,%rbx
   53d8c:	jne    5412b <clockcache_get+0x4cb>
   53d92:	mov    %r13,%r12
   53d95:	mov    %rax,%rbx
   53d98:	mov    %rdx,%r13
   53d9b:	jmp    53dad <clockcache_get+0x14d>
   53d9d:	nopl   (%rax)
   53da0:	mov    %r12,%rdi
   53da3:	call   ea40 <clockcache_wait@plt>
   53da8:	mov    0x48(%r12),%rcx
   53dad:	mov    0x10(%rcx,%r13,1),%eax
   53db2:	test   $0x10,%al
   53db4:	jne    53da0 <clockcache_get+0x140>
   53db6:	mov    0x48(%r12),%rdx
   53dbb:	mov    %rbx,%rax
   53dbe:	add    %r13,%rdx
   53dc1:	mov    0x14(%rdx),%r9d
   53dc5:	test   %r9d,%r9d
   53dc8:	jne    5429d <clockcache_get+0x63d>
   53dce:	mov    0x20(%rdx),%eax
   53dd1:	cmp    %eax,0xc(%rsp)
   53dd5:	jne    542d9 <clockcache_get+0x679>
   53ddb:	mov    0x8(%r12),%rax
   53de0:	mov    0x18(%rax),%r11d
   53de4:	test   %r11d,%r11d
   53de7:	jne    54106 <clockcache_get+0x4a6>
   53ded:	mov    0x10(%rsp),%rbx
   53df2:	mov    %rdx,(%rbx)
   53df5:	data16 lea 0x821ab(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53dfd:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53e05:	xor    %r9d,%r9d
   53e08:	mov    (%rax),%rax
   53e0b:	mov    %rax,0x8(%rbx)
   53e0f:	add    $0x38,%rsp
   53e13:	mov    %r9d,%eax
   53e16:	pop    %rbx
   53e17:	pop    %rbp
   53e18:	pop    %r12
   53e1a:	pop    %r13
   53e1c:	pop    %r14
   53e1e:	pop    %r15
   53e20:	ret
   53e21:	nopl   0x0(%rax)
   53e28:	mov    %r14d,%esi
   53e2b:	mov    %r13,%rdi
   53e2e:	call   aa4b0 <clockcache_try_get_read.constprop.0>
   53e33:	cmp    $0x1,%eax
   53e36:	jne    53d6e <clockcache_get+0x10e>
   53e3c:	xor    %eax,%eax
   53e3e:	xchg   %ax,%ax
   53e40:	pause
   53e42:	mov    %rax,%rdx
   53e45:	add    $0x1,%rax
   53e49:	cmp    %rdx,%r12
   53e4c:	jne    53e40 <clockcache_get+0x1e0>
   53e4e:	jmp    53e28 <clockcache_get+0x1c8>
   53e50:	call   aa4b0 <clockcache_try_get_read.constprop.0>
   53e55:	cmp    $0x1,%eax
   53e58:	je     53f10 <clockcache_get+0x2b0>
   53e5e:	cmp    $0x2,%eax
   53e61:	jne    53d76 <clockcache_get+0x116>
   53e67:	mov    0x8(%r13),%rax
   53e6b:	mov    0x8(%rax),%rdx
   53e6f:	mov    0x8(%rdx),%rdx
   53e73:	lea    -0x1(%rdx),%rcx
   53e77:	test   %rbx,%rcx
   53e7a:	je     53cc0 <clockcache_get+0x60>
   53e80:	mov    0x82149(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53e87:	mov    %rdx,%rcx
   53e8a:	lea    0x6541f(%rip),%rsi        # b92b0 <__func__.21+0xe20>
   53e91:	mov    %rbx,%rdx
   53e94:	xor    %eax,%eax
   53e96:	mov    0x0(%rbp),%rdi
   53e9a:	call   fbf0 <fprintf@plt>
   53e9f:	mov    0x0(%rbp),%rdi
   53ea3:	call   fe90 <fflush@plt>
   53ea8:	mov    $0x16,%r9d
   53eae:	jmp    53e0f <clockcache_get+0x1af>
   53eb3:	nopl   0x0(%rax,%rax,1)
   53eb8:	mov    0x10(%r13),%r14
   53ebc:	mov    (%r14),%rax
   53ebf:	mov    %r14,%rdi
   53ec2:	call   *(%rax)
   53ec4:	xor    %edx,%edx
   53ec6:	mov    %rax,%rcx
   53ec9:	mov    (%rax),%rax
   53ecc:	mov    0x8(%rax),%r9
   53ed0:	mov    %rbx,%rax
   53ed3:	div    %r9
   53ed6:	test   %rdx,%rdx
   53ed9:	jne    540d0 <clockcache_get+0x470>
   53edf:	mov    (%r14),%rax
   53ee2:	mov    %rbx,%r15
   53ee5:	and    0x20(%rcx),%r15
   53ee9:	je     53f20 <clockcache_get+0x2c0>
   53eeb:	cmp    0x8(%rcx),%rbx
   53eef:	jae    53f20 <clockcache_get+0x2c0>
   53ef1:	mov    %r15,%rsi
   53ef4:	mov    %r14,%rdi
   53ef7:	call   *0x20(%rax)
   53efa:	test   %eax,%eax
   53efc:	je     54180 <clockcache_get+0x520>
   53f02:	mov    0x8(%rsp),%r8d
   53f07:	test   %r8d,%r8d
   53f0a:	jne    53fa1 <clockcache_get+0x341>
   53f10:	mov    $0xb,%r9d
   53f16:	jmp    53e0f <clockcache_get+0x1af>
   53f1b:	nopl   0x0(%rax,%rax,1)
   53f20:	mov    %r14,%rdi
   53f23:	call   *0x58(%rax)
   53f26:	mov    0x820a3(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53f2d:	mov    %rbx,%r9
   53f30:	mov    %r15,%r8
   53f33:	sub    $0x8,%rsp
   53f37:	lea    0x64622(%rip),%rsi        # b8560 <__func__.21+0xd0>
   53f3e:	mov    $0x17d,%ecx
   53f43:	mov    0x0(%rbp),%rdi
   53f47:	push   %rax
   53f48:	lea    0x6f9a1(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   53f4f:	xor    %eax,%eax
   53f51:	call   fbf0 <fprintf@plt>
   53f56:	mov    0x0(%rbp),%rdi
   53f5a:	call   fe90 <fflush@plt>
   53f5f:	pop    %rsi
   53f60:	pop    %rdi
   53f61:	jmp    53ea8 <clockcache_get+0x248>
   53f66:	cs nopw 0x0(%rax,%rax,1)
   53f70:	imul   %rax,%rdx
   53f74:	mov    0x82055(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   53f7b:	lea    0x65366(%rip),%rsi        # b92e8 <__func__.21+0xe58>
   53f82:	xor    %eax,%eax
   53f84:	mov    0x0(%rbp),%rdi
   53f88:	mov    %rdx,%rcx
   53f8b:	mov    %rbx,%rdx
   53f8e:	call   fbf0 <fprintf@plt>
   53f93:	mov    0x0(%rbp),%rdi
   53f97:	call   fe90 <fflush@plt>
   53f9c:	jmp    53ea8 <clockcache_get+0x248>
   53fa1:	data16 lea 0x81fff(%rip),%rdi        # d5fa8 <xxxtid@@Base+0xd5fa8>
   53fa9:	data16 data16 rex.W call eda0 <__tls_get_addr@plt>
   53fb1:	mov    0xc(%rsp),%edx
   53fb5:	mov    %rbx,%rsi
   53fb8:	mov    %r13,%rdi
   53fbb:	mov    (%rax),%r12
   53fbe:	mov    0x8(%r13),%rax
   53fc2:	mov    0x8(%rax),%rax
   53fc6:	mov    0x8(%rax),%r15
   53fca:	call   53830 <clockcache_acquire_entry_for_load.lto_priv.0>
   53fcf:	mov    %rax,%r14
   53fd2:	mov    $0xffffffff,%eax
   53fd7:	cmp    %rax,%r14
   53fda:	je     53e67 <clockcache_get+0x207>
   53fe0:	mov    0x8(%r13),%rax
   53fe4:	mov    %r14,%rbp
   53fe7:	mov    %r12,(%rsp)
   53feb:	mov    %r13,%r12
   53fee:	shl    $0x6,%rbp
   53ff2:	add    0x48(%r13),%rbp
   53ff6:	mov    0x18(%rax),%ecx
   53ff9:	test   %ecx,%ecx
   53ffb:	jne    54227 <clockcache_get+0x5c7>
   54001:	mov    0x18(%r12),%rdi
   54006:	mov    0x0(%rbp),%rsi
   5400a:	mov    %rbx,%rcx
   5400d:	mov    %r15,%rdx
   54010:	mov    (%rdi),%rax
   54013:	call   *(%rax)
   54015:	mov    %eax,%r9d
   54018:	test   %eax,%eax
   5401a:	jne    5424e <clockcache_get+0x5ee>
   54020:	mov    0x8(%r12),%rax
   54025:	mov    0x18(%rax),%edx
   54028:	test   %edx,%edx
   5402a:	jne    541c1 <clockcache_get+0x561>
   54030:	mov    %r9d,%ecx
   54033:	mov    %r14d,%edx
   54036:	mov    %rbx,%rsi
   54039:	mov    %r12,%rdi
   5403c:	mov    %r9d,0x8(%rsp)
   54041:	call   539c0 <clockcache_finish_load.lto_priv.0>
   54046:	mov    0x8(%rsp),%r9d
   5404b:	mov    %rbp,%rdx
   5404e:	test   %r9d,%r9d
   54051:	je     53ded <clockcache_get+0x18d>
   54057:	mov    0x8(%r12),%rcx
   5405c:	mov    %r14,%rax
   5405f:	xor    %edx,%edx
   54061:	mov    (%rsp),%rdi
   54065:	divq   0x140(%rcx)
   5406c:	and    $0x3,%edi
   5406f:	shl    $0x6,%edx
   54072:	lea    (%rdx,%rax,1),%esi
   54075:	mov    0x130(%rcx),%eax
   5407b:	mov    $0xffffffff,%ecx
   54080:	mov    0x88(%r12),%rdx
   54088:	imul   %rdi,%rax
   5408c:	add    %rsi,%rax
   5408f:	lock xadd %cx,(%rdx,%rax,2)
   54095:	test   %cx,%cx
   54098:	jne    53e0f <clockcache_get+0x1af>
   5409e:	push   %rax
   5409f:	push   %rdi
   540a0:	push   %rsi
   540a1:	push   0x18(%rsp)
   540a5:	xor    %r9d,%r9d
   540a8:	lea    0x650d9(%rip),%r8        # b9188 <__func__.21+0xcf8>
   540af:	xor    %eax,%eax
   540b1:	lea    0x5e576(%rip),%rcx        # b262e <_fini+0x1552>
   540b8:	lea    0x6f971(%rip),%rdx        # c3a30 <__func__.28.lto_priv.1>
   540bf:	mov    $0x27c,%esi
   540c4:	lea    0x5e50c(%rip),%rdi        # b25d7 <_fini+0x14fb>
   540cb:	call   ee00 <platform_assert_false@plt>
   540d0:	mov    0x81ef9(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   540d7:	mov    %rbx,%r8
   540da:	xor    %eax,%eax
   540dc:	mov    $0x164,%ecx
   540e1:	lea    0x6f808(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   540e8:	lea    0x643b9(%rip),%rsi        # b84a8 <__func__.21+0x18>
   540ef:	mov    0x0(%rbp),%rdi
   540f3:	call   fbf0 <fprintf@plt>
   540f8:	mov    0x0(%rbp),%rdi
   540fc:	call   fe90 <fflush@plt>
   54101:	jmp    53ea8 <clockcache_get+0x248>
   54106:	mov    (%rsp),%rsi
   5410a:	mov    0xc(%rsp),%ecx
   5410e:	lea    0x0(,%rsi,8),%rax
   54116:	sub    %rsi,%rax
   54119:	lea    0x220(%rcx,%rax,8),%rax
   54121:	addq   $0x1,(%r12,%rax,8)
   54126:	jmp    53ded <clockcache_get+0x18d>
   5412b:	mov    0x8(%r13),%rcx
   5412f:	xor    %edx,%edx
   54131:	mov    (%rsp),%r8
   54135:	divq   0x140(%rcx)
   5413c:	and    $0x3,%r8d
   54140:	shl    $0x6,%edx
   54143:	lea    (%rdx,%rax,1),%edi
   54146:	mov    0x130(%rcx),%eax
   5414c:	mov    0x88(%r13),%rdx
   54153:	mov    $0xffffffff,%ecx
   54158:	imul   %r8,%rax
   5415c:	add    %rdi,%rax
   5415f:	lock xadd %cx,(%rdx,%rax,2)
   54165:	test   %cx,%cx
   54168:	jne    53e67 <clockcache_get+0x207>
   5416e:	push   %rbx
   5416f:	push   %r8
   54171:	push   %rdi
   54172:	jmp    540a1 <clockcache_get+0x441>
   54177:	nopw   0x0(%rax,%rax,1)
   54180:	sub    $0x8,%rsp
   54184:	mov    %rbx,%r9
   54187:	mov    %r15,%r8
   5418a:	mov    $0x172,%ecx
   5418f:	mov    0x81e3a(%rip),%rbp        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   54196:	lea    0x6f753(%rip),%rdx        # c38f0 <__func__.22.lto_priv.0>
   5419d:	lea    0x64354(%rip),%rsi        # b84f8 <__func__.21+0x68>
   541a4:	mov    0x0(%rbp),%rdi
   541a8:	push   $0x0
   541aa:	call   fbf0 <fprintf@plt>
   541af:	mov    0x0(%rbp),%rdi
   541b3:	call   fe90 <fflush@plt>
   541b8:	pop    %r9
   541ba:	pop    %r10
   541bc:	jmp    53ea8 <clockcache_get+0x248>
   541c1:	lea    0x20(%rsp),%rsi
   541c6:	mov    $0x1,%edi
   541cb:	mov    %r9d,0x8(%rsp)
   541d0:	call   e410 <clock_gettime@plt>
   541d5:	mov    0x18(%rsp),%rax
   541da:	mov    (%rsp),%rsi
   541de:	imul   $0x3b9aca00,0x20(%rsp),%rdx
   541e7:	add    0x28(%rsp),%rdx
   541ec:	sub    %rax,%rdx
   541ef:	mov    0xc(%rsp),%ecx
   541f3:	lea    0x0(,%rsi,8),%rax
   541fb:	mov    0x8(%rsp),%r9d
   54200:	sub    %rsi,%rax
   54203:	lea    (%rcx,%rax,8),%rax
   54207:	lea    (%r12,%rax,8),%rax
   5420b:	addq   $0x1,0x1148(%rax)
   54213:	addq   $0x1,0x1220(%rax)
   5421b:	add    %rdx,0x1190(%rax)
   54222:	jmp    54030 <clockcache_get+0x3d0>
   54227:	lea    0x20(%rsp),%rsi
   5422c:	mov    $0x1,%edi
   54231:	call   e410 <clock_gettime@plt>
   54236:	imul   $0x3b9aca00,0x20(%rsp),%rax
   5423f:	add    0x28(%rsp),%rax
   54244:	mov    %rax,0x18(%rsp)
   54249:	jmp    54001 <clockcache_get+0x3a1>
   5424e:	mov    %eax,%edi
   54250:	mov    %eax,0x8(%rsp)
   54254:	call   efe0 <strerror@plt>
   54259:	mov    0x81d70(%rip),%r15        # d5fd0 <Platform_error_log_handle@@Base-0x1368>
   54260:	mov    0xc(%rsp),%ecx
   54264:	mov    %rbx,%rdx
   54267:	mov    %rax,%r8
   5426a:	lea    0x65657(%rip),%rsi        # b98c8 <__func__.21+0x1438>
   54271:	xor    %eax,%eax
   54273:	mov    (%r15),%rdi
   54276:	call   fbf0 <fprintf@plt>
   5427b:	mov    (%r15),%rdi
   5427e:	call   fe90 <fflush@plt>
   54283:	mov    0x8(%r12),%rax
   54288:	mov    0x8(%rsp),%r9d
   5428d:	mov    0x18(%rax),%edx
   54290:	test   %edx,%edx
   54292:	je     54030 <clockcache_get+0x3d0>
   54298:	jmp    541c1 <clockcache_get+0x561>
   5429d:	mov    0x8(%r12),%rcx
   542a2:	jmp    5405f <clockcache_get+0x3ff>
   542a7:	sub    $0x8,%rsp
   542ab:	lea    0x653b6(%rip),%r8        # b9668 <__func__.21+0x11d8>
   542b2:	xor    %eax,%eax
   542b4:	push   %rsi
   542b5:	mov    0x1c(%rsp),%r9d
   542ba:	lea    0x653cf(%rip),%rcx        # b9690 <__func__.21+0x1200>
   542c1:	lea    0x6f5f8(%rip),%rdx        # c38c0 <__func__.23.lto_priv.1>
   542c8:	mov    $0x1ca,%esi
   542cd:	lea    0x5e303(%rip),%rdi        # b25d7 <_fini+0x14fb>
   542d4:	call   ee00 <platform_assert_false@plt>
   542d9:	mov    0xc(%rsp),%esi
   542dd:	mov    %r14d,%r9d
   542e0:	lea    0x5e423(%rip),%r8        # b270a <_fini+0x162e>
   542e7:	lea    0x5e433(%rip),%rcx        # b2721 <_fini+0x1645>
   542ee:	lea    0x6f4db(%rip),%rdx        # c37d0 <__func__.19>
   542f5:	lea    0x5e2db(%rip),%rdi        # b25d7 <_fini+0x14fb>
   542fc:	push   %rsi
   542fd:	mov    $0x8d1,%esi
   54302:	push   %rax
   54303:	xor    %eax,%eax
   54305:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
