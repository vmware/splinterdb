
.cache/invalid-address-perf-20261017/builds/main/release/lib/libsplinterdb.so:     file format elf64-x86-64


Disassembly of section .init:

Disassembly of section .plt:

Disassembly of section .plt.got:

Disassembly of section .text:

0000000000060d60 <core_range_iterator_next>:
   60d60:	push   %rbx
   60d61:	mov    0x43c(%rdi),%eax
   60d67:	test   %eax,%eax
   60d69:	je     60d92 <core_range_iterator_next+0x32>
   60d6b:	mov    %rdi,%rbx
   60d6e:	mov    0x430(%rdi),%rdi
   60d75:	mov    (%rdi),%rax
   60d78:	call   *0x18(%rax)
   60d7b:	test   %eax,%eax
   60d7d:	jne    60d90 <core_range_iterator_next+0x30>
   60d7f:	mov    %rbx,%rdi
   60d82:	pop    %rbx
   60d83:	jmp    60050 <core_range_iterator_next.part.0>
   60d88:	nopl   0x0(%rax,%rax,1)
   60d90:	pop    %rbx
   60d91:	ret
   60d92:	lea    0x507e1(%rip),%r8        # b157a <_fini+0x99e>
   60d99:	lea    0x50e8a(%rip),%rcx        # b1c2a <_fini+0x104e>
   60da0:	xor    %eax,%eax
   60da2:	mov    $0x9f4,%esi
   60da7:	lea    0x62262(%rip),%rdx        # c3010 <__func__.55>
   60dae:	lea    0x50a1a(%rip),%rdi        # b17cf <_fini+0xbf3>
   60db5:	call   ee00 <platform_assert_false@plt>

Disassembly of section .fini:
